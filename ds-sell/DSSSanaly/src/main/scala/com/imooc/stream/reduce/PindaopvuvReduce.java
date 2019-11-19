package com.imooc.stream.reduce;

import com.youfan.analy.PindaoPvUv;
import com.youfan.analy.PindaoRD;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * Created by Administrator on 2018/10/28 0028.
 */
public class PindaopvuvReduce implements ReduceFunction<PindaoPvUv> {

    @Override
    public PindaoPvUv reduce(PindaoPvUv value1, PindaoPvUv value2) throws Exception {
        PindaoPvUv pindaoPvUv = new PindaoPvUv();
        System.out.println("value1=="+value1);
        System.out.println("value2=="+value2);
        pindaoPvUv.setPingdaoid(value1.getPingdaoid());

        return  pindaoPvUv;
    }
}
