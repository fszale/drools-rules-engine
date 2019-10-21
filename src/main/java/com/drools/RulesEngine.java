package com.drools;

import java.io.Serializable;

import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.AgendaFilter;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.runtime.rule.Match;
import org.kie.api.runtime.Globals;
import org.kie.api.runtime.KieContainer;
import org.kie.api.io.KieResources;
import org.kie.api.io.ResourceType;
import org.kie.api.builder.*;
import org.kie.api.definition.KiePackage;
import org.kie.api.definition.type.FactType;
import org.kie.api.event.rule.AfterMatchFiredEvent;
import org.kie.api.event.rule.AgendaEventListener;
import org.kie.api.event.rule.DefaultAgendaEventListener;

import java.io.File;
import java.io.InputStream;
// import java.io.Serializable;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import org.apache.log4j.PropertyConfigurator;
import org.drools.core.spi.Activation;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RulesEngine implements Serializable {

	private Logger logger = LoggerFactory.getLogger("RulesEngine");
	private KieServices ks = null;
	private KieContainer kContainer = null;
	private KieSession kSession = null;
	private KieRepository kr = null;
	private ArrayList<Object> objects = new ArrayList<Object>();
	private ArrayList<String> ruleslog = new ArrayList<String>();
	private String dataLineageId = "";
	private String runActivityId = "";
	private DefaultAgendaEventListener agendaEventListener = null;

	public String toString(){
		return String.format("dataLineageId: %s runActivityId: %s", dataLineageId, runActivityId);
	}

	public RulesEngine(String ruleset) {
		this(ruleset, null, null);
	}

	public RulesEngine(JSONObject config) {
		this(null, config, null);
	}
	
	public RulesEngine(String ruleset, JSONObject config, Logger logger) {

		if(logger != null) {
			this.logger = logger;
		}
		
		// TODO : process configuration object
		HashMap<String, String> ruleFiles = new HashMap<String, String>();
		try {

			if (ruleset == null || ruleset.isEmpty()) {
				ruleset = "rulesengine_default";
			}
			
			Properties properties = new Properties();

			// first check to see if properties file asked for lives outside of the jar
			File jarPath = new File(RulesEngine.class.getProtectionDomain().getCodeSource().getLocation().getPath());
			String propertiesPath = jarPath.getParentFile().getAbsolutePath();
			propertiesPath = propertiesPath + "/" + ruleset + ".properties";
			File prf = new File(propertiesPath);
			if(prf.isFile()) {				
				properties.load(new FileInputStream(prf.getAbsolutePath()));
			} else {
				// if the properties file does not exist outside of the jar then we need to look inside
				propertiesPath = "/" + ruleset + ".properties";
				properties.load(getClass().getResourceAsStream(propertiesPath));
			}
			PropertyConfigurator.configure(properties);
			String path = properties.getProperty("rules.files.path");
			
			if (path != null && !path.isEmpty()) {
				File f = new File(path);
				File[] matchingFiles = f.listFiles();
				for (int off = 0; off < matchingFiles.length; off++) {
					logger.info("Rules engine attempting to load  file " + matchingFiles[off].getAbsolutePath().toString());
					if(matchingFiles[off].getName().endsWith("drl"))
					{
						logger.info("Rules engine loading DROOL file " + matchingFiles[off].getAbsolutePath().toString());
						ruleFiles.put(matchingFiles[off].getName(), matchingFiles[off].getAbsolutePath());
					}
					else
					{
						logger.info("Rules engine ignoring non-DROOL file " + matchingFiles[off].getAbsolutePath().toString());
					}
				}
			}
			else {
				logger.error("EXCEPTION loading properties for Drools rules engine at path:\n" + path);
			}

		} catch (Exception e) {
			logger.error("EXCEPTION loading properties for Drools rules engine\n" + e.getMessage());
			e.printStackTrace();

		}

		// spin up rules engine
		try {

			logger.info("Rules engine initializing 0.0 ...");
			ks = KieServices.Factory.get();
			kr = ks.getRepository();
			kContainer = this.createKieContainer(kr.getDefaultReleaseId(), ruleFiles);
			logger.info("Rules engine initializing 0.1 ...");
			
			agendaEventListener = new DefaultAgendaEventListener() {

				public void afterMatchFired(AfterMatchFiredEvent event) {
					super.afterMatchFired(event);
					String msg = "[RULE_ENGINE] : {";								
					msg += !dataLineageId.isEmpty() ? "\"dataLineageId\":\"" + dataLineageId + "\"," : ""; 
					msg += !runActivityId.isEmpty() ? "\"runActivityId\":\"" + runActivityId + "\"," : "";							
					msg += "\"Package\":\"" + event.getMatch().getRule().getPackageName() + "\",\"Rule\":\"" + event.getMatch().getRule().getName() + "\"}";						
					ruleslog.add(msg);
				}
			};

			logger.info("Rules engine initialized.");
			
		} catch (Exception e) {
			logger.error("EXCEPTION initializing Drools rules engine\n" + e.getMessage());
			e.printStackTrace();

		}
	}

	public void setdataLineageId(String dataLineageId) {
		this.dataLineageId = dataLineageId;
	}

	public void setrunActivityId(String runActivityId) {
		this.runActivityId = runActivityId;
	}

	public Object objectsCreate(String name) {
		return this.objectsCreate(null, name);
	}

	public Object objectsCreate(String packageName, String name) {
		Object ret = null;

		if (packageName == null || packageName.isEmpty()) {
			packageName = "com.signavio.droolsexport";
		}

		try {
			
			KieBase b = kSession.getKieBase();
			FactType enums = b.getFactType(packageName, name);
			ret = enums.newInstance();

		} catch (Exception e) {
			logger.error("EXCEPTION creating object in Drools rules engine\n" + e.getMessage());
			e.printStackTrace();
		}

		return ret;
	}

	public void sessionCreate() {

		// to do: move the session to class creation to save execution time
		kSession = kContainer.newKieSession();
		kSession.addEventListener(agendaEventListener);

	}

	private KieContainer createKieContainer(ReleaseId releaseId, HashMap<String, String> ruleFiles) {

		logger.info("CK CreateKieContainer");

		final KieFileSystem kfs = ks.newKieFileSystem();

		for (String ruleKey : ruleFiles.keySet()) {
			logger.info("CK Processing ruleKey: " + ruleKey);
			try {
				String ruleFile = ruleFiles.get(ruleKey);
				InputStream ruleFileIs = null;
				try {
					ruleFileIs = new FileInputStream(new File(ruleFile));
					if(ruleFileIs == null) {
						ruleFileIs = getClass().getResourceAsStream(ruleFile);
					}
				} catch(Exception ex) {
					ruleFileIs = getClass().getResourceAsStream(ruleFile);
				}
				if (ruleFileIs != null) {
					kfs.write(ks.getResources().newInputStreamResource(ruleFileIs).setSourcePath(ruleKey));
				}
				if(ruleFileIs != null) {
					ruleFileIs.close();
					logger.info("CK Resolved ruleKey: " + ruleKey);
				} else {
					logger.info("CK EXCEPTION Unable to load ruleKey: " + ruleKey);
					logger.error("CK EXCEPTION Unable to load ruleKey: " + ruleKey);
				}
			} catch (IOException e) {
				logger.info("CK EXCEPTION Unresolved ruleKey: " + ruleKey + e.toString());
				logger.error("CK EXCEPTION Processing ruleKey: " + ruleKey);
				throw new RuntimeException("CK EXCEPTION InputStream\n" + e.toString());
			}
		}

		KieRepository kieRepository = ks.getRepository();

		logger.info("CK Adding module.");
		kieRepository.addKieModule(new KieModule() {
			public ReleaseId getReleaseId() {
				return kieRepository.getDefaultReleaseId();
			}
		});

		logger.info("CK Building ...");
		// Create the builder for the resources of the File System
		KieBuilder kieBuilder = null;
		try {
			kieBuilder = ks.newKieBuilder(kfs).buildAll();
		} catch (Exception e) {
			logger.info("CK EXCEPTION building rules\n" + e.getMessage());
			logger.error("CK EXCEPTION building rules\n" + e.getMessage());
			throw new RuntimeException("CK EXCEPTION building rules\n" + e.toString());
		}
		logger.info("CK Building Complete.");
		
		// Check for errors
		if (kieBuilder.getResults().hasMessages(Message.Level.ERROR)) {
			logger.info("CK EXCEPTION building: " + kieBuilder.getResults().toString());
			logger.error("CK EXCEPTION building: " + kieBuilder.getResults().toString());
			throw new RuntimeException(kieBuilder.getResults().toString());
		}

		logger.info("CK Return new container.");
		// Create the Container, wrapping the KieModule with the given ReleaseId
		return ks.newKieContainer(kieRepository.getDefaultReleaseId());
	}

	public void objectsAdd(Object obj) {
		objects.add(obj);
	}

	public void objectsEmpty() {

		objects = new ArrayList<Object>();

		// do not recreate the session, simply remove the facts for speed of execution
		// remove any of the previously utilized objects
		for (FactHandle f : kSession.getFactHandles()) {
			kSession.delete(f);
		}
	}

	public ArrayList<Object> objectsGet() {

		return objects;
	}

	public ArrayList<String> getRulesLog() {

		return ruleslog;
	}

	public ArrayList<Object> objectsFind(String endsWithName) {

		ArrayList<Object> ret = new ArrayList<Object>();
		for (FactHandle f : kSession.getFactHandles()) {
			if (kSession.getObject(f).getClass().getSimpleName().endsWith(endsWithName)) {
				ret.add(kSession.getObject(f));
			}
		}
		return ret;

	}

	public boolean runRules() {
		return runRules("");
	}

	public boolean runRules(String filter) {

		try {
			// load objects into the rules engine
			for (Object o : objects) {
				kSession.insert(o);
			}

			// initialize logs
			ruleslog = new ArrayList<String>();
			
			if(filter != null && !filter.isEmpty()) {
				
				kSession.fireAllRules(new AgendaFilter() {
		            @Override
		            public boolean accept(Match match) {
		                String packageName = match.getRule().getPackageName();

		                if (packageName.equals(filter)) {
		                    return true;
		                }

		                return false;
		            }
		        });
				
			} else {
				kSession.fireAllRules();
			}

		} catch (Exception e) {

			logger.error("EXCEPTION running rules : " + e.toString());
			return false;
		}

		return true;
	}
}