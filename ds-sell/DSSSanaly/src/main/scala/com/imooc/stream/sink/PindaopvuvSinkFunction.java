package com.imooc.stream.sink;

import com.imooc.util.HbaseUtil;
import com.youfan.analy.PindaoPvUv;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.HashMap;
import java.util.Map;

public class PindaopvuvSinkFunction implements SinkFunction<PindaoPvUv> {

    @Override
    public void invoke(PindaoPvUv value, Context context) throws Exception {
        long pingdaoid = value.getPingdaoid();
        long pvcount = value.getPvcount();
        long uvcount = value.getUvcount();
        String timestring = value.getTimestring();
        String pv = HbaseUtil.getdata("pindaoinfo", pingdaoid + timestring, "info", "pv");
        String uv = HbaseUtil.getdata("pindaoinfo", pingdaoid + timestring, "info", "uv");
        if(StringUtils.isNotBlank(pv)){
            pvcount = pvcount + Long.valueOf(pv);
        }
        if(StringUtils.isNotBlank(uv)){
            uvcount = uvcount + Long.valueOf(uv);
        }
        Map<String,String> data = new HashMap<>();
        data.put("pv",pvcount+"");
        data.put("uv",uvcount+"");
        HbaseUtil.put("pindaoinfo",pingdaoid+timestring,"info",data);
    }
}
