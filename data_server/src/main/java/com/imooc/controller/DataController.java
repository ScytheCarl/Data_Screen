package com.imooc.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.imooc.utils.RedisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * 数据接口V1.0
 * Created by xuwei
 */
@RestController//控制器类
@RequestMapping("/v1")//映射路径
public class DataController {
    private static final Logger logger = LoggerFactory.getLogger(DataController.class);
    /**
     * 测试接口
     * @param name
     * @return
     */
    @RequestMapping(value="/t1",method = RequestMethod.GET)
    public String test(@RequestParam("name") String name) {

        return "hello,"+name;
    }

    /**
     * 查询GMV
     * @return
     */
    @RequestMapping(value="/getGmv",method = RequestMethod.GET)
    public JSONArray getGmv() {
        JSONArray resArr = new JSONArray();
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("name","");
        try{
            Jedis jedis = RedisUtils.getJedis();
            long gmv = Long.parseLong(jedis.get("gmv"));
            jsonObj.put("value",gmv);
            RedisUtils.returnResource(jedis);
        }catch (Exception e){
            logger.error(e.getMessage());
        }
        resArr.add(jsonObj);
        return resArr;
    }


    /**
     * 查询TopN商品品类
     * @return
     */
    @RequestMapping(value="/getTopN",method = RequestMethod.GET)
    public JSONArray getTopN() {
        JSONArray resArr = new JSONArray();
        try{
            Jedis jedis = RedisUtils.getJedis();
            Set<String> topTypes = jedis.zrevrange("goods_type", 0, 4);
            Iterator<String> it = topTypes.iterator();
            while(it.hasNext()){
                String type = it.next();
                JSONObject jsonObj = new JSONObject();
                jsonObj.put("商品品类",type);
                resArr.add(jsonObj);
            }
            RedisUtils.returnResource(jedis);
        }catch (Exception e){
            logger.error(e.getMessage());
        }
        return resArr;
    }





}
