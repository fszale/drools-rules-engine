package com;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.drools.RunRules;

public class RulesEngineTest {
		
    public static final void main(String[] args) {
        try {
            
        	Logger logger = LoggerFactory.getLogger("test-logger");
        	
        	HashMap<String,String> structFields = new HashMap<String,String>();
        	structFields.put("ruleconfiguration", "{\"Object\":\"House\","
        			+ "\"Package\":\"com.rules.test123\","
        			+ "\"Wrapper\":\"Input\","
        			+ "\"dataLineageId\":\"123.0\","
        			+ "\"runActivityId\":\"ABCDEF\","
        			+ "\"Ruleset\":\"prop_dataimpute\"}");
        	structFields.put("id", "1");
        	structFields.put("bedrooms", "3");
        	structFields.put("sqfootage", "2000");

        	Map<String,String> ret = new HashMap<String,String>();       	
        	ArrayList<String> runningLog = new ArrayList<String>();
        	
        	RunRules rr = new RunRules();
        	rr.setLogger(logger);
        	
        	ret = rr.call(structFields);
        	
        	for (Map.Entry<String, String> kv : ret.entrySet()) {
        		logger.info("RET : " + kv.getKey() + " - " + kv.getValue());
        	}        	
        	
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

}

