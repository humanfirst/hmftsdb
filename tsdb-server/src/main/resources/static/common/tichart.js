if(Date.prototype.format==null){
	Date.prototype.format = function(format){
		var o = {
			"M+" : this.getMonth()+1, //month
			"d+" : this.getDate(),    //day
			"h+" : this.getHours(),   //hour
			"m+" : this.getMinutes(), //minute
			"s+" : this.getSeconds(), //second
			"q+" : Math.floor((this.getMonth()+3)/3),  //quarter
			"S" : this.getMilliseconds() //millisecond
		}
		if(/(y+)/.test(format)) 
			format=format.replace(RegExp.$1,(this.getFullYear()+"").substr(4 - RegExp.$1.length));
		for(var k in o)
			if(new RegExp("("+ k +")").test(format))
				format = format.replace(RegExp.$1,RegExp.$1.length==1 ? o[k] :("00"+ o[k]).substr((""+ o[k]).length));
		return format;
	}
}
var KLine = function(containerId){
	this.containerId = containerId;
	this.zr = zrender.init(document.getElementById(containerId));	
	this.hourLen = 3600*1000;
	this.dayLen = 24* this.hourLen;
	this.weekLen = this.dayLen*7;
	this.pHeight = this.zr.getHeight();
	this.pWidth = this.zr.getWidth();
	this.rightMargin = 60;
	this.bottomMargin = 64;
	this.drawDefaultAxis=function(){
		var xAxis = new zrender.Line({
			shape:{
				x1:0,
				y1:this.pHeight - this.bottomMargin,
				x2:this.pWidth-this.rightMargin,
				y2:this.pHeight-this.bottomMargin
			},
			style:{
				stroke:'black',
				strokeWidth:1
			}
		})
		this.zr.add(xAxis);
		var yAxis = new zrender.Line({
			shape:{
				x1:this.pWidth-this.rightMargin,
				y1:0,
				x2:this.pWidth-this.rightMargin,
				y2:this.pHeight-this.bottomMargin
			},
			style:{
				stroke:'black',
				strokeWidth:1
			}
		})
		this.zr.add(yAxis);
	}
	this.drawDefaultAxis();
};

KLine.prototype={
	getTimeStr:function(t){
		var d = new Date(t);
		var s = d.format("hh:mm");
		if(s=="00:00"){
			s = d.format("hh:mm\r\nMM-dd");
		}
		return s;
	},
	//根据最大值确定纵坐标刻度范围：1,2,5,10,20,50,100
	getScale:function(min,max){
		if(max<0){
			max = 0;
		}
		if(min>0)
			min = 0;
			
		var vlen = max-min;
		if(vlen==0){
			return {min:0,max:1,len:0.2};
		}
		
		var s = vlen / 5;
		if(s>=5)
			s = Math.round(s);		
		else{
			s = Math.round(s*10)/10;
		}
		var l = (""+s).length;		
		var littleS = s / Math.pow(10,l-1);
		if(littleS<1.5){
			littleS = 1;
		}else if(littleS<3.5){
			littleS = 2
		}else if(littleS<7.5){
			littleS = 5
		}else
			littleS = 10
		s = littleS * Math.pow(10,l-1);
		
		if(min % s !=0){
			min = min - (s+min%s);
		}
		if(max%s!=0){
			max = max+(s-max%s);
		}
		return {min:min,max:max,len:s};		
	},	
	getMax:function(points){
		var max = null;
		for(var i=0;i<points.length;i++){
			var cMax = points[i][1];
			if(max==null)
				max = cMax;
			else if(cMax>max)
				max = cMax;
		}
		return max*1.1;
	},
	clear:function(){
		this.zr.clear();
		this.xRate = null;
		this.yRate = null;
		this.pointWidth = null;
		/*
		$("#"+this.containerId).find("canvas").off("mousedown");
		$("#"+this.containerId).find("canvas").off("mouseup");
		$("#"+this.containerId).find("canvas").off("mousemove");
		*/
		
	},
	
	getPointWidth:function(start,end,klParameter){
		if(this.pointWidth==null){
			var pointTimeWidth = klParameter.timeLenInMS;
			var w = pointTimeWidth * (this.pWidth-this.rightMargin) / (end-start);
		
			if(w<3){
				w = 3;
			}else if(w>=50){
				w = 49;
			}
			this.pointWidth = w;
		}
		return this.pointWidth;
	},
	
	drawPoint:function(point){
		//计算左中右的x坐标值
		
		
		//绘制线条
		if(this.xRate ==null){
			this.xRate = this.x + (this.right - this.x)/(this.end - this.start);
			this.yRate = (this.bottom - this.top)/(this.max - this.min);
		}
		
		var t = point[0];
		var x1 = (t-this.start)*this.xRate;
		var x2 = x1 + this.getPointWidth()/2;
		var x3 = x1 + this.getPointWidth();
		
		//计算最大、最小、起始、结束的y坐标值
		var y1 = this.bottom - (point[1] - this.top)*this.yRate;
		var y2 = this.bottom - (point[2] - this.top)*this.yRate;
		var y3 = this.bottom - (point[3] - this.top)*this.yRate;
		var y4 = this.bottom - (point[4] - this.top)*this.yRate;
		var color = "red";
		if(point[4]==point[3])
			color = "black";
		else if(point[4]<point[3])
			color = "green";
		
		var height = y4-y3;
		if(height<1)
			height = 1;
		//绘制矩形
		var rect = new zrender.Rect({
			shape:{
				x:x1,
				y:y3,
				width:this.getPointWidth(),
				height:height
			},
			style:{
				fill:color,
				lineWidth:0
			}
		});
		this.zr.add(rect);
		
	},
	
	draw:function(data){
		this.clear();
		var max = this.getMax(data.points);
		var min = 0;		
		var points = data.points;
		var pointWidth = this.getPointWidth(data.start,data.end,data.parameters);
		var start = data.end - data.parameters.timeLenInMS* (this.pWidth-this.rightMargin)/pointWidth;		
		this.drawAxis(start,data.end,max,min);	
		for(var i=0;i<points.length;i++){
			var p = points[i];
			this.drawPoint(p);
		}
		/*
		var points = new Array();
		for(key in dps){
			var t = key*1;
			var value = dps[key];
			if(t<this.end && t>this.start)
				points.push(this.calcPosition(t,value));
		}
		if(this.pointsOfTSD==null){
			this.pointsOfTSD = [];
		}
		this.pointsOfTSD[index] = points;
		var polyline = new zrender.Polyline({
			shape:{
				points:points,
				smooth:0
			},
			style:{
				stroke:this.colors[index],
				lineWidth:2
			}
		});
		this.zr.add(polyline);*/
		
	},
	format:function(v){
		v = v+"";
		var p = v.indexOf("00000");
		if(p>0){
			v = v.substring(0,p);
		}
		return v;
	},
	roundToDayLen:function(ms){
		return 24*3600*1000*Math.round(ms/(24*3600*1000));
	},
	drawAxis:function(startTime,endTime,max,min){
		var xAxis = new zrender.Line({
			shape:{
				x1:0,
				y1:this.pHeight - this.bottomMargin,
				x2:this.pWidth-this.rightMargin,
				y2:this.pHeight-this.bottomMargin
			},
			style:{
				stroke:'black',
				strokeWidth:1
			}
		})
		this.zr.add(xAxis);
		var yAxis = new zrender.Line({
			shape:{
				x1:this.pWidth-this.rightMargin,
				y1:0,
				x2:this.pWidth-this.rightMargin,
				y2:this.pHeight-this.bottomMargin
			},
			style:{
				stroke:'black',
				strokeWidth:1
			}
		})
		this.zr.add(yAxis);
		
		//纵向刻度,todo:尽可能向整数靠
		var scale = this.getScale(min,max);
		min = scale.min; max = scale.max;
		var bottom = this.pHeight-this.bottomMargin;
		var vLen = scale.len;
		
		this.start = startTime;
		this.end = endTime;
		this.min = min;
		this.max = max;
		this.x = 0 ;
		this.bottom = bottom;
		this.top = 0
		this.right = this.pWidth-this.rightMargin
		
		for(var i=0;i<=max;i+=vLen){
			var y = bottom-i*bottom/(max-min);
			var yScaleLabel = new zrender.Line({
				shape:{
					x1:0,
					y1:y,
					x2:this.pWidth-this.rightMargin+5,
					y2:y
				},
				style:{
					stroke:'gray',
					strokeWidth:0.5
				}
			})
			this.zr.add(yScaleLabel);
			
			var yScaleText = new zrender.Text({
				style:{
					text:this.format(i),
					fontFamily:"宋体",
					fontSize:12,
					textFill:"black",								
				}
			});
			if(i==max)
				yScaleText.attr('position', [this.pWidth-this.rightMargin+8, y+6]);
			else
				yScaleText.attr('position', [this.pWidth-this.rightMargin+8, y-6]);
			this.zr.add(yScaleText);
		}
		
		//横轴刻度
		var timeLen = endTime - startTime;
		
		var sLen = 0;
		if(timeLen<=this.hourLen){ 
			//根据timeLen 的值,分成分钟、5分钟、10分钟这3种刻度
			if(timeLen<=10*60*1000){
				sLen = 60*1000;
			}else if(timeLen>10*60*1000 && timeLen<50*60*1000){
				sLen = 5*60*1000
			}else{
				sLen = 10*60*1000;
			}
		}else if(timeLen>this.hourLen && timeLen<=this.dayLen){
			//分成10分钟、30分钟、1小时、2小时 这4种刻度
			if(timeLen<=100*60*1000)
				sLen = 10*60*1000;
			else if(timeLen<=300*60*1000)
				sLen = 30*60*1000;
			else if(timeLen<=3600*1000*10)
				sLen = 3600*1000;
			else 
				sLen = 3600*1000*2;
			
		}else if(timeLen>this.dayLen && timeLen<=this.weekLen){
			//分成4小时、12小时、24小时这3种刻度
			if(timeLen<=10*4*3600*1000){
				sLen = 4*3600*1000;
			}else if(timeLen<10*12*3600*1000){
				sLen = 12*3600*1000;
			}else {
				sLen = 24*3600*1000;
			}
		}else if(timeLen>this.weekLen ){
			//分成n天的刻度，控制总可刻度数不超过10
			if(timeLen<=10*this.dayLen){
				sLen = this.dayLen;
			}else{
				sLen = timeLen / 10;
				sLen = roundToDayLen(sLen);
			}
		}
		//确定第一个刻度位置
		var v = (startTime +8*3600000)% sLen;
		
		var first = startTime;
		if(v>0){
			first = startTime+sLen-v;
		}
		
		for(var t =first;t<=endTime;t+=sLen){
			var x = (t-startTime)*(this.pWidth - this.rightMargin)/(endTime-startTime);
			
			var xScaleLabel = new zrender.Line({
				shape:{
					x1:x,
					y1:bottom+5,
					x2:x,
					y2:0
				},
				style:{
					stroke:'gray',
					strokeWidth:0.5
				}
			})
			this.zr.add(xScaleLabel);
			var labelStr = this.getTimeStr(t);
			var xScaleText = new zrender.Text({
				style:{
					text:labelStr,
					fontFamily:"宋体",
					fontSize:12,
					textFill:"black",								
				}
			});
			if(x<12)
				x = 12;
			xScaleText.attr('position', [x-12, bottom+8])
			this.zr.add(xScaleText);
		}
	}
}

var MyChart = function(containerId){
	this.containerId = containerId;
	this.zr = zrender.init(document.getElementById(containerId));	
	this.hourLen = 3600*1000;
	this.dayLen = 24* this.hourLen;
	this.weekLen = this.dayLen*7;
	this.pHeight = this.zr.getHeight();
	this.pWidth = this.zr.getWidth();
	this.rightMargin = 60;
	this.bottomMargin = 64;
	this.colors = ["red","green","orange","blue","yellow","rgb(63,72,204)","rgb(163,73,164)","rgb(136,0,21)","rgb(185,122,87)","rgb(255,174,201)","rgb(255,201,14)","rgb(239,228,176)","rgb(181,230,29)","rgb(153,217,234)","rgb(112,146,190)","rgb(200,191,231)"];
	
	this.focusedPoint = null;
	
	$(document.body).append('<div id="flag4CurrentPoint" style="position:absolute;  display:none; border-style: solid; white-space: nowrap; z-index: 9999999; background-color: rgba(30, 30, 30, 0.8); border-width: 0px; border-color: rgb(51, 51, 51); border-radius: 4px; color: rgb(255, 255, 255); font: 14px/21px &quot;Microsoft YaHei&quot;; padding: 5px; left: 722px; top: 228px;">2018-10-10 10:10:10:23.456</div>');
	$("#flag4CurrentPoint").css("left",0);
	$("#flag4CurrentPoint").css("top",0);
	this.drawDefaultAxis=function(){
		var xAxis = new zrender.Line({
			shape:{
				x1:0,
				y1:this.pHeight - this.bottomMargin,
				x2:this.pWidth-this.rightMargin,
				y2:this.pHeight-this.bottomMargin
			},
			style:{
				stroke:'black',
				strokeWidth:1
			}
		})
		this.zr.add(xAxis);
		var yAxis = new zrender.Line({
			shape:{
				x1:this.pWidth-this.rightMargin,
				y1:0,
				x2:this.pWidth-this.rightMargin,
				y2:this.pHeight-this.bottomMargin
			},
			style:{
				stroke:'black',
				strokeWidth:1
			}
		})
		this.zr.add(yAxis);
	}
	this.drawDefaultAxis();
}
MyChart.prototype={
	getTimeStr:function(t){
		var d = new Date(t);
		var s = d.format("hh:mm");
		if(s=="00:00"){
			s = d.format("hh:mm\r\nMM-dd");
		}
		return s;
	},
	//根据最大值确定纵坐标刻度范围：1,2,5,10,20,50,100
	getScale:function(min,max){
		if(max<0){
			max = 0;
		}
		if(min>0)
			min = 0;
			
		var vlen = max-min;
		if(vlen==0){
			return {min:0,max:1,len:0.2};
		}
		
		var s = vlen / 5;	
		s = Math.round(s);		
		/*
		if(s>=5)
			s = Math.round(s);		
		else{
			s = Math.round(s*10)/10;
		}*/
		var l = (""+s).length;		
		var littleS = s / Math.pow(10,l-1);
		if(littleS<1.5){
			littleS = 1;
		}else if(littleS<3.5){
			littleS = 2
		}else if(littleS<7.5){
			littleS = 5
		}else
			littleS = 10
		s = littleS * Math.pow(10,l-1);
		
		if(min % s !=0){
			min = min - (s+min%s);
		}
		if(max%s!=0){
			max = max+(s-max%s);
		}
		return {min:min,max:max,len:s};		
	},	
	getMax:function(data){
		var max = null;
		var dps = data.values;
		for(key in dps){
			if(max==null)
				max = dps[key];
			else if(dps[key]>max)
				max = dps[key];
		}
		return max*1.1;
	},
	clear:function(){
		$("#"+this.containerId).find("canvas").off("mousedown");
		$("#"+this.containerId).find("canvas").off("mouseup");
		$("#"+this.containerId).find("canvas").off("mousemove");
		this.focusedPoint = null;
		this.zr.clear();
		
	},
	
	move:function(from ,to ){
		var offset = from - to ;
		var xLen = this.right;
		var timeLen = this.originTimeEnd - this.originTimeStart;
		var timeOffset = offset *timeLen/xLen;
		var end = this.originTimeEnd + timeOffset;
		var start = this.originTimeStart + timeOffset;
		this.data[0].start = start;
		this.data[0].end = end;
		this.draw(this.data);
	},
	moveEnd:function(from ,to){
		var offset = from - to ;
		var xLen = this.right;
		var timeLen = this.originTimeEnd - this.originTimeStart;
		var timeOffset = offset *timeLen/xLen;
		var end = Math.round(this.originTimeEnd + timeOffset);
		var start = Math.round(this.originTimeStart + timeOffset);
		this.moveTo(start,end);
	},
	getMaxFromMultiTSD:function(data){
		var max4All = null;
		for(var i=0;i<data.length;i++){
			var max = this.getMax(data[i]);
			if(max4All==null){
				max4All = max;
			}else if(max>max4All){
				max4All = max;
			}
		}
		return max4All;
	},
	drawTitles:function(data){
		var titles4Metrics = $("#titles4Metrics");
		if(titles4Metrics.length==0){
			$(document.body).append('<div id="titles4Metrics" style="position:absolute; white-space: nowrap; z-index: 9999999; font: 14px/21px &quot;Microsoft YaHei&quot;flex-flow:row;justify-content:center;"></div>');
			titles4Metrics = $("#titles4Metrics");
		}else{
			titles4Metrics.html("");
		}
		titles4Metrics.css("left",0);
		var canvas = $("#"+this.containerId)[0];
		var top = canvas.offsetTop+$(canvas).height()-32;
		titles4Metrics.css("top",top);
		titles4Metrics.css("width","100%");
		titles4Metrics.css("height","24px");
		titles4Metrics.css("line-height","24px");
		titles4Metrics.css("display","flex");
		
		for(var i=0;i<data.length;i++){
			var one = data[i];
			var metric = one.metric;
			var tags = one.tags;
			var title = new Array();
			title.push('<div style="margin:4px;">');
			title.push('<span class="glyphicon glyphicon-flag" aria-hidden="true" style="color:'+this.colors[i]+'"></span>');			
			title.push(this.drawTitle(one));			
			title.push('</div>');
			titles4Metrics.append($(title.join("")));
		}
	},
	drawTitle:function(series){
		return (series.metric+"@"+series.tags["deviceId"]);
	},
	draw:function(data){
		this.drawTitles(data);
		var flag = $("#flag4CurrentPoint");
		flag.css("display","none");
		this.xRate = null;
		this.data = data;
		var max = this.getMaxFromMultiTSD(data);
		var min = 0;	
		this.clear();
		
		
		if(data.start!=null)
			this.drawAxis(data.start,data.end,max,min);
		else
			this.drawAxis(data[0].start,data[0].end,max,min);	
		this.drawMultiLines(data);
		
		$("#"+this.containerId).find("canvas").ready(function(chart){
			return function(){
				var canvas = $("#"+chart.containerId).find("canvas");
				canvas.on("mousedown",function(e){
					var x = e.originalEvent.offsetX;
					var y = e.originalEvent.offsetY;
					if(x>chart.right || y >chart.bottom){
						return ;
					}
					$(this).css("cursor","move");
					chart.dragStartAt=x;
					chart.draging = true;
					chart.originTimeStart = chart.start;
					chart.originTimeEnd = chart.end;

					if(chart.focusedPoints!=null){
						for(var i=0;i<chart.focusedPoints.length;i++){
							if(chart.focusedPoints[i]!=null)
								chart.zr.remove(chart.focusedPoints[i]);
						}
						chart.zr.remove(chart.focusedTimeline);
						chart.zr.flush();
						chart.focusedTimeline = null;
						chart.focusedPoints = null;
						var flag = $("#flag4CurrentPoint");
						flag.css("display","none");
					}
				});
				canvas.on("mouseup",function(e){	
					if(chart.draging){
						var x = e.originalEvent.offsetX;
						chart.dragEndAt = x;
						chart.moveEnd(chart.dragStartAt,chart.dragEndAt);
						$("#"+chart.containerId).width();
						setTimeout(function(){
							chart.draging = false;
							$("#"+chart.containerId).find("canvas").css("cursor","default");		
						},100);
					}
					window.getSelection().removeAllRanges() ;
				});
				canvas.on("mouseout",function(){
					if(chart.focusedPoints!=null){
						for(var i=0;i<chart.focusedPoints.length;i++){
							if(chart.focusedPoints[i]!=null)
								chart.zr.remove(chart.focusedPoints[i]);
						}
						chart.zr.remove(chart.focusedTimeline);
						
						chart.zr.flush();
						chart.focusedTimeline = null;
						chart.focusedPoints = null;
						var flag = $("#flag4CurrentPoint");
						flag.css("display","none");
					}
				});
				
				canvas.on("mousemove",function(e){
					
					var x = e.originalEvent.offsetX;
					var y = e.originalEvent.offsetY;
					
					if(x<chart.right && y <chart.bottom){
						
						if(chart.draging==true){
							var x = e.originalEvent.offsetX;
							chart.dragEndAt = x;
							if(chart.preDragAt==null || Math.abs(chart.dragEndAt-chart.preDragAt)>1){
								setTimeout(function(){
									chart.move(chart.dragStartAt,chart.dragEndAt);
								},10);
								
								chart.preDragAt = x;
							}else{
							}
							return;
						}
						
						var points = chart.getPointsByX(x);
						if(points==null || points.length==0){
							return;
						}
						var nearPoint = null;
						for(var i=0;i<points.length;i++){
							if(points[i]!=null){
								nearPoint = points[i];
								break;
							}
						}
						if(chart.focusedPoints==null){
							for(var i = 0;i<points.length;i++){
								var circle = new zrender.Circle({
									shape:{
										cx:points[i][0],
										cy:points[i][1],
										r:2											
									},
									style:{
										stroke:'red',
										fill:'red',
										strokeWidth:1
									}
								})
								chart.zr.add(circle);
								if(chart.focusedPoints==null)
									chart.focusedPoints = new Array();
								chart.focusedPoints[i] = circle;
							}
							
							
							var timeLine = new zrender.Line({
								shape:{
									x1:nearPoint[0],
									y1:0,
									x2:nearPoint[0],
									y2:chart.pHeight - chart.bottomMargin
								},
								style:{
									stroke:'gray',
									strokeWidth:0.5
								}
							});
							chart.zr.add(timeLine);
							chart.focusedTimeline = timeLine;
						}else{
							for(var i = 0;i<points.length;i++){
								if(points[i]!=null){
									chart.focusedPoints[i].shape.cx = points[i][0];
									chart.focusedPoints[i].shape.cy = points[i][1];
									chart.focusedPoints[i].dirty();
								}
							}
							chart.focusedTimeline.shape.x1 = nearPoint[0];
							chart.focusedTimeline.shape.x2 = nearPoint[0];
							chart.focusedTimeline.dirty();
							chart.zr.flush();
						}
						//显示当前值：
						var flag = $("#flag4CurrentPoint");
						flag.html(chart.flagInfo(points));
						var w = flag.width(),h = flag.height();
						var x = e.pageX+15,y = e.pageY+15;
						if(x+w>$(document.body).width()){
							x = e.pageX - w - 15;
						}
						if(y+h>$(document.body).height()){
							y = e.pageY - h - 15;
						}
						
						flag.css("display","block");
						flag.css("left",x);
						flag.css("top",y);		
						
					}
				})
			};
			
		}(this));
	},
	getPointsByX:function(x){
		var r = [];
		for(var i=0;i<this.data.length;i++){
			var points = this.pointsOfTSD[i];
			var point = this.getPointByX(x,points);
			if(point!=null)
				r[i] = point;
		}
		return r;
	},
	getPointByX:function(x,points){
		if(points.length<1)
			return null;
		var r = new Array();
		var cx = 0;
		var start=0,end=points.length-1;
		for(;;){
			if(end-start>=2){
				var mid = start + Math.floor((end-start)/2);
				cx = points[mid][0];
				if(cx==x){
					start = mid;
					end = mid;
					break;
				}else if(cx>x){
					end = mid;
				}else {
					start = mid;
				}
			}else
				break;
		}
		var index = 0;
		if(start!=end){
			var delta1 = Math.abs(points[start][0]-x);
			var delta2 = Math.abs(points[end][0]-x);
			index = delta1<=delta2?start:end;
		}else
			index = start;
		return points[index];
	},
	
	calcPosition:function(t,v){
		if(this.xRate ==null){
			this.xRate = this.x + (this.right - this.x)/(this.end - this.start);
			this.yRate = (this.bottom - this.top)/(this.max - this.min);
		}
		
		var x = (t-this.start)*this.xRate;
		var y = this.bottom - (v - this.top)*this.yRate;
		return [x,y,t,v];
	},
	drawMultiLines:function(data){
		this.focusedPoints = null;
		for(var i=0;i<data.length;i++){
			var dps = data[i].values;
			this.drawLines(i,dps);
		}
	},
	drawLines:function(index,dps){
		var points = new Array();
		for(key in dps){
			var t = key*1;
			var value = dps[key];
			if(t<this.end && t>this.start)
				points.push(this.calcPosition(t,value));
		}
		if(this.pointsOfTSD==null){
			this.pointsOfTSD = [];
		}
		this.pointsOfTSD[index] = points;
		var polyline = new zrender.Polyline({
			shape:{
				points:points,
				smooth:0
			},
			style:{
				stroke:this.colors[index],
				lineWidth:2
			}
		});
		this.zr.add(polyline);
	},
	format:function(v){
		v = v+"";
		var p = v.indexOf("00000");
		if(p>0){
			v = v.substring(0,p);
		}
		return v;
	},
	roundToDayLen:function(ms){
		return 24*3600*1000*Math.round(ms/(24*3600*1000));
	},
	drawAxis:function(startTime,endTime,max,min){
		var xAxis = new zrender.Line({
			shape:{
				x1:0,
				y1:this.pHeight - this.bottomMargin,
				x2:this.pWidth-this.rightMargin,
				y2:this.pHeight-this.bottomMargin
			},
			style:{
				stroke:'black',
				strokeWidth:1
			}
		})
		this.zr.add(xAxis);
		var yAxis = new zrender.Line({
			shape:{
				x1:this.pWidth-this.rightMargin,
				y1:0,
				x2:this.pWidth-this.rightMargin,
				y2:this.pHeight-this.bottomMargin
			},
			style:{
				stroke:'black',
				strokeWidth:1
			}
		})
		this.zr.add(yAxis);
		
		
		
		//纵向刻度,todo:尽可能向整数靠
		var scale = this.getScale(min,max);
		min = scale.min; max = scale.max;
		var bottom = this.pHeight-this.bottomMargin;
		var vLen = scale.len;
		
		this.start = startTime;
		this.end = endTime;
		this.min = min;
		this.max = max;
		this.x = 0 ;
		this.bottom = bottom;
		this.top = 0
		this.right = this.pWidth-this.rightMargin
		
		for(var i=0;i<=max;i+=vLen){
			var y = bottom-i*bottom/(max-min);
			var yScaleLabel = new zrender.Line({
				shape:{
					x1:0,
					y1:y,
					x2:this.pWidth-this.rightMargin+5,
					y2:y
				},
				style:{
					stroke:'gray',
					strokeWidth:0.5
				}
			})
			this.zr.add(yScaleLabel);
			
			var yScaleText = new zrender.Text({
				style:{
					text:this.format(i),
					fontFamily:"宋体",
					fontSize:12,
					textFill:"black",								
				}
			});
			if(i==max)
				yScaleText.attr('position', [this.pWidth-this.rightMargin+8, y+6]);
			else
				yScaleText.attr('position', [this.pWidth-this.rightMargin+8, y-6]);
			this.zr.add(yScaleText);
		}
		
		//横轴刻度
		var timeLen = endTime - startTime;
		
		var sLen = 0;
		if(timeLen<=this.hourLen){ 
			//根据timeLen 的值,分成分钟、5分钟、10分钟这3种刻度
			if(timeLen<=10*60*1000){
				sLen = 60*1000;
			}else if(timeLen>10*60*1000 && timeLen<50*60*1000){
				sLen = 5*60*1000
			}else{
				sLen = 10*60*1000;
			}
		}else if(timeLen>this.hourLen && timeLen<=this.dayLen){
			//分成10分钟、30分钟、1小时、2小时 这4种刻度
			if(timeLen<=100*60*1000)
				sLen = 10*60*1000;
			else if(timeLen<=300*60*1000)
				sLen = 30*60*1000;
			else if(timeLen<=3600*1000*10)
				sLen = 3600*1000;
			else 
				sLen = 3600*1000*2;
			
		}else if(timeLen>this.dayLen && timeLen<=this.weekLen){
			//分成4小时、12小时、24小时这3种刻度
			if(timeLen<=10*4*3600*1000){
				sLen = 4*3600*1000;
			}else if(timeLen<10*12*3600*1000){
				sLen = 12*3600*1000;
			}else {
				sLen = 24*3600*1000;
			}
		}else if(timeLen>this.weekLen ){
			//分成n天的刻度，控制总可刻度数不超过10
			if(timeLen<=10*this.dayLen){
				sLen = this.dayLen;
			}else{
				sLen = timeLen / 10;
				sLen = this.roundToDayLen(sLen);
			}
		}
		//确定第一个刻度位置
		var v = (startTime +8*3600000)% sLen;
		
		var first = startTime;
		if(v>0){
			first = startTime+sLen-v;
		}
		
		for(var t =first;t<=endTime;t+=sLen){
			var x = (t-startTime)*(this.pWidth - this.rightMargin)/(endTime-startTime);
			
			var xScaleLabel = new zrender.Line({
				shape:{
					x1:x,
					y1:bottom+5,
					x2:x,
					y2:0
				},
				style:{
					stroke:'gray',
					strokeWidth:0.5
				}
			})
			this.zr.add(xScaleLabel);
			var labelStr = this.getTimeStr(t);
			var xScaleText = new zrender.Text({
				style:{
					text:labelStr,
					fontFamily:"宋体",
					fontSize:12,
					textFill:"black",								
				}
			});
			if(x<12)
				x = 12;
			xScaleText.attr('position', [x-12, bottom+8])
			this.zr.add(xScaleText);
		}
	}
}