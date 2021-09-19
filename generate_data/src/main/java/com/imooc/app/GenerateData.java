package com.imooc.app;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

/**
 * 模拟产生测试订单数据
 * 数据格式如下：
 * {
 * 	"orderNo": "9fc3ffe3-3255-414e-8df8-1580cfbd2ff9",//订单编号
 * 	"totalPrice": 11237,//订单总金额
 * 	"detal": [{//订单明细
 * 		"goodsNo": "6002",//商品编号
 * 		"goodsCount": 1,//商品数量
 * 		"goodsPrice": 3299,//商品单价
 * 		"goodsName": "美的 606升",//商品名称
 * 		"goodsType": "冰箱"//商品品类
 *        }, {
 * 		"goodsNo": "1005",
 * 		"goodsCount": 2,
 * 		"goodsPrice": 3969,
 * 		"goodsName": "荣耀 30Pro",
 * 		"goodsType": "手机"
 *    }]
 * }
 * Created by xuwei
 */
public class GenerateData {
    private static Logger logger = LoggerFactory.getLogger(GenerateData.class);

    public static void main(String[] args) throws Exception{
        //默认的间隔时间
        long millis = 1000;
        if(args.length>0){
            millis = Long.parseLong(args[0]);
        }
        //读取基础测试数据
        String fileName = "goodsInfo.dat";
        InputStream in = GenerateData.class.getClassLoader().getResourceAsStream(fileName);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        ArrayList<String> orderArr = new ArrayList<String>();
        String line;
        while ((line = br.readLine()) != null) {
            orderArr.add(line);
        }
        br.close();

        //模拟(随机)生成订单数据
        Random random = new Random();
        while(true){
            JSONObject jsonObj = new JSONObject();
            jsonObj.put("orderNo", UUID.randomUUID());
            //汇总订单总金额
            int totalPrice = 0;
            //生成订单明细
            JSONArray detal = new JSONArray();
            int randOrderCount = random.nextInt(5)+1;
            while (randOrderCount>0){
                JSONObject orderJson = new JSONObject();
                int n = random.nextInt(orderArr.size());
                String orderStr = orderArr.get(n);
                //1001,华为 mate40,手机,7499
                String[] splits = orderStr.split(",");
                String goodsNo = splits[0];
                String goodsName = splits[1];
                String goodsType = splits[2];
                int goodsPrice = Integer.parseInt(splits[3]);
                int goodsCount = random.nextInt(2)+1;
                totalPrice+=goodsPrice*goodsCount;
                orderJson.put("goodsNo",goodsNo);
                orderJson.put("goodsName",goodsName);
                orderJson.put("goodsType",goodsType);
                orderJson.put("goodsPrice",goodsPrice);
                orderJson.put("goodsCount",goodsCount);
                detal.add(orderJson);
                randOrderCount--;
            }
            jsonObj.put("detal",detal);
            jsonObj.put("totalPrice",totalPrice);
            logger.info(jsonObj.toJSONString());
            Thread.sleep(millis);
        }
    }
}
