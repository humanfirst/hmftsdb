package org.hmf.tsdb.server.ql;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hmf.tsdb.DownSample;
import org.hmf.tsdb.server.ql.func.FuncExp;

public class Compiler {
	private static Date pareseFrom(String str) throws Exception {
		str = str.substring(1, str.length()-1);
		Calendar now = Calendar.getInstance();
		str  = str.toLowerCase();
		String number = str.substring(0, str.length()-1);
		int n = Integer.valueOf(number);
		if(str.endsWith("h")) {
			now.add(Calendar.HOUR_OF_DAY, 0-n);
		}else if(str.endsWith("m")) {
			now.add(Calendar.MINUTE, 0-n);
		}else if(str.endsWith("d")) {
			now.add(Calendar.DATE, 0-n);
		}else 
			throw new Exception("无法将字符串解析成开始时间:"+str);
		
		return now.getTime();
	}
	
	
	public static DownSample parseDS(String str) {
		str = str.substring(1, str.length()-1);
		DownSample ds = new DownSample();
		String[] ss = str.split(":");
		String p = ss[0];
		String intOfP = p.substring(0,p.length()-1);
		String arithmetics = ss[1];
		ds.setFrequence(Integer.parseInt(intOfP));
		if(p.endsWith("h")) {		
			ds.setUnit(TimeUnit.HOURS);
		}else if(p.endsWith("m")) {
			ds.setUnit(TimeUnit.MINUTES);
		}else if(p.endsWith("s")) {
			ds.setUnit(TimeUnit.SECONDS);
		}
		ss = arithmetics.split(",");
		for(String s:ss) {
			ds.addArithmetic(DownSample.Arithmetic.valueOf(s));
		}
		return ds;
	}
	public static List<TagExp> parseTags(String str){
		List<TagExp> list = new ArrayList<TagExp>();
		String[] ss = str.split(",");
		for(String s :ss) {
			String metric ,value,symbol;
			int p = s.indexOf("!=");
			if(p>0) {
				metric = s.substring(0, p);
				value = s.substring(p+2);
				symbol = "!=";
			}else {
				p = s.indexOf("=");
				metric = s.substring(0, p);
				value = s.substring(p+1);
				symbol = "=";
			}
			TagExp tag = new TagExp();
			tag.setName(metric);
			tag.setSymbol(symbol);

			String[] vs = value.split("\\|");
			for(String v:vs) {
				value = cutQuote(v);
				tag.addValue(value);
			}
			list.add(tag);
		}
		return list;
	}
	
	private static String cutQuote(String str) {
		str = str.replaceAll("\"", "'");
		if(str.indexOf("'")>-1) {
			return str.split("'")[1];
		}else
			return str;
	}
	
	private static void collectWord(StringBuilder word,List<String> collector) {
		if(word!=null && word.length()>0) {
			collector.add(word.toString());
			word.delete(0, word.length());
		}
	}
	
	private static List<String> parseToWords(String str){
		List<String> result = new ArrayList<String>();
		StringBuilder word = new StringBuilder();
		boolean isTags = false;
		boolean isStr = false;
		
		for(int i=0;i<str.length();i++) {
			char c = str.charAt(i);
			if(isTags) {
				word.append(c);
				if(c=='}') {
					collectWord(word,result);
					isTags = false;
					
				}
			}else {
				if(c=='{') {
					collectWord(word,result);
					word.append(c);
					isTags = true;
				}else {
					if(isStr) {
						word.append(c);
						if(c=='\'') {
							collectWord(word,result);
							isStr = false;
						}
					}else {
						if(c=='\'') {
							collectWord(word,result);
							word.append(c);
							isStr = true;
						}else {
							//即不在{}中，也不再‘’中
							if(c=='[') {								
								collectWord(word,result);
								word.append(c);
								for(;;) {
									i++;
									c = str.charAt(i);
									word.append(c);
									if(c==']') {
										collectWord(word,result);
										break;
									}
								}
							}else if(c==',') {
								collectWord(word,result);
								word.append(c);
								collectWord(word,result);
							}else if(c==' ') {
								collectWord(word,result);
							}else if(c=='(' || c==')') {
								collectWord(word,result);
								word.append(c);
								collectWord(word,result);
							}else {
								word.append(c);
							}
						}
					}
				}
			}
		}
		return result;
	}
	
	public static Exp compile(String str) throws Exception {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Exp current = null;
		
		List<String> words = parseToWords(str);
		int closeCount = 0;
		for(int i=0;i<words.size();i++) {
			String w = words.get(i);
			if(w.equals("(")) {
				String preWord = i==0?"join":words.get(i-1);
				FuncExp one = FuncExp.build(preWord);
				if(current!=null) {
					one.parent = current;
					current.addChild(one);
				}
				current = one;
			}else if(w.equals(")")){
				closeCount++;
				if(current instanceof FuncExp==false) {
					current = current.getParent();
				}else {
					if(closeCount==2) {
						current = current.getParent();
						closeCount = 0;
					}
				}
				
			}else if(w.startsWith("{")) {
				SelectorExp selector = new SelectorExp();
				selector.setMetric(words.get(i-1));
				String tagStr = w;
				tagStr = tagStr.substring(1, tagStr.length()-1);
				selector.setTags(parseTags(tagStr));
				if(current!=null) {
					current.addChild(selector);
					selector.setParent(current);
				}
				current = selector;
			}else if(w.equals(",")) {
				current = current.getParent();
				closeCount = 0;
			}else if(w.equals("by")) {
				((FuncExp)current).addBy(words.get(++i));				
			}else if(w.equals("from")) {
				String start = words.get(++i);
				start = Compiler.cutQuote(start);
				Date d1 = df.parse(start);
				current.setFrom(d1);
			}else if(w.equals("to")){
				String end = words.get(++i);
				end = Compiler.cutQuote(end);
				Date d2 = df.parse(end);
				current.setTo(d2);
			}else if(w.startsWith("[")) {
				String preWord = words.get(i-1);
				if(preWord.equals("downsample")) {
					DownSample ds = parseDS(w);
					current.setDs(ds);
				}else if(preWord.equals("last")) {
					Date from = pareseFrom(w);
					current.setFrom(from);
				}
			}
		}
		
		Exp p = null;
		while((p=current.getParent())!=null) {
			current = p;
			break;
		}
		return current;
	}
	
	
	public static void main(String[] args) throws Exception {
		Exp exp = null;
		String str = null;
		
		 
		
		str = "avg(温度{type='室内环境温度',area='机房1'|'机房2'})";
		exp = Compiler.compile(str);
		System.out.println(exp);
		
		str = "max(温度{type='室内环境温度'})  ";
		exp = Compiler.compile(str);
		System.out.println(exp);
		
		str = " sub(max(温度{type='室内环境'}),min(温度{type='室内环境'})) by room downsample[1m:avg] from '2018-09-12 10:00:00' to '2018-09-12 20:00:00' ";
		exp = Compiler.compile(str);
		System.out.println(exp);
		
		str = "max(温度{type='室内环境'}) by room downsample[1m:avg] last[1h]";
		exp = Compiler.compile(str);
		System.out.println(exp);
		
		str = "max(温度{type='室内环境'}) by room downsample[1m:avg] from '2018-09-20 10:00:00'";
		exp = Compiler.compile(str);
		System.out.println(exp);
		
		str = "电流{device='配电柜1',port='输入'} kline[1h] last[5d]"; 
		exp = Compiler.compile(str);
		System.out.println(exp);
	}
}
