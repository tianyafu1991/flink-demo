package com.imooc.stream.reduce;

import com.youfan.analy.Userbrowser;
import com.youfan.analy.Usernetwork;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * Created by Administrator on 2018/10/28 0028.
 */
public class UserbrowserReduce implements ReduceFunction<Userbrowser> {

    @Override
    public Userbrowser reduce(Userbrowser value1,Userbrowser value2) throws Exception {
        System.out.println( "value1=="+value1);
        System.out.println( "value2=="+value2);

        long timestampvalue = value1.getTimestamp();
        String timestring = value1.getTimestring();
        long countvalue1 = value1.getCount();
        long newcountvalue1 = value1.getNewcount();
        long oldcountvalue1 = value1.getOldcount();

        long countvalue2 = value2.getCount();
        long newcountvalue2 = value2.getNewcount();
        long oldcountvalue2 = value2.getOldcount();

        Userbrowser userbrowser = new Userbrowser();
        userbrowser.setTimestring(timestring);
        userbrowser.setTimestamp(timestampvalue);
        userbrowser.setOldcount(oldcountvalue1+oldcountvalue2);
        userbrowser.setNewcount(newcountvalue1+newcountvalue2);
        userbrowser.setCount(countvalue1+countvalue2);

        System.out.println( "recuduce --userbrowser=="+userbrowser);
        return  userbrowser;
    }
}
