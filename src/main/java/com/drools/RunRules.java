package com.drools;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunRules {

	private static final long serialVersionUID = 1L;
	private static final HashMap<String, RulesEngine> regroup = new HashMap<String, RulesEngine>();
	private Logger logger = LoggerFactory.getLogger("RunRules");
	
	private static final String convert(Object str) {
		if (str != null && !str.toString().isEmpty())
			return str.toString();
		return "";
	}

	public void setLogger(Logger logger) {
		this.logger = logger;
	}
	
	public Map call(HashMap<String, String> row) throws Exception {
		HashMap<String, String> ret = new HashMap<String, String>();

		if (row == null)
			return ret;
		
		try {
			RulesEngine re = null;
			String objectName = null;
			String objectWrapper = null;
			String dataLineageId = "";
			String runActivityId = "";
			String ruleset = "";
			String objectPackage = null;
			String config = row.get("ruleconfiguration");
			
			if (config != null) {
				JSONObject jObject = new JSONObject(config);
				objectName = jObject.getString("Object");
				objectPackage = jObject.getString("Package");
				objectWrapper = jObject.getString("Wrapper");
				dataLineageId = jObject.has("dataLineageId") ? jObject.getString("dataLineageId") : "";
				runActivityId = jObject.has("runActivityId") ? jObject.getString("runActivityId") : "";
				ruleset = jObject.getString("Ruleset");			
				if(!regroup.containsKey(ruleset)) {
					RulesEngine rnew = new RulesEngine(ruleset, null, this.logger);
					regroup.put(ruleset, rnew);
				}
				re = regroup.get(ruleset);			
				re.setdataLineageId(dataLineageId);
				re.setrunActivityId(runActivityId);
				logger.info("RulesEngine obj created " + re.toString());
			}
	
			if (objectName == null || re == null) {
				logger.error("Missing ruleconfiguration:object, cannot run rules");
				return ret;
			}
	
			// create new session, but this will need to change to gain execution efficiency
			re.sessionCreate();
			
			// good to run, start mapping data to setters
			Object o = re.objectsCreate(objectPackage, objectName);
			Class c = o.getClass();
	
			for (Map.Entry<String, String> field : row.entrySet()) {
				if (field.getKey().toLowerCase() != "ruleconfiguration") {
					try {
						for (Method m : c.getDeclaredMethods()) {
							if (m.getName().toLowerCase().equalsIgnoreCase("set" + field.getKey())) {
								// found a match between incoming property and a class setter,
								// good to use it
								Object val = row.get(field.getKey());
								if(val != null) {
									if(m.getParameters()[0].getType() == BigDecimal.class) {
										if(!val.toString().isEmpty())
											m.invoke(o, new BigDecimal(val.toString()) );
									} else {
										m.invoke(o, RunRules.convert(val.toString()));
									}
								}
								break;
							}
						}
					} catch (Exception e) {
						logger.error("Could not invoke setter for field " + field.getKey() + ":" + e);
					}
				}
			}
			
			// add objects to rules engine
			re.objectsEmpty();
			
			// if a wrapper object (for signavio) has been specified, then create it
			Class cwrap = null;
			Object owrap = null;
			if (objectWrapper != null) {			
				owrap = re.objectsCreate(objectPackage, objectWrapper);
				cwrap = owrap.getClass();
				try {
					for (Method m : cwrap.getDeclaredMethods()) {
						if (m.getName().toLowerCase().equalsIgnoreCase("set" + objectName)) {
							// found a match between incoming object class and a class setter,
							// in the wrapper object, so set the variable in the wrapper
							m.invoke(owrap, o);
							break;
						}
					}
				} catch (Exception e) {
					logger.error("Could not invoke setter of " + objectName + " for " + objectWrapper + ":" + e);
				}			
				re.objectsAdd(owrap);
			}else {
				re.objectsAdd(o);
			}		
	
			// run rules
			re.runRules(objectPackage);
	
			// dump logs to info
			for (String s : re.getRulesLog()) {
				logger.info(s);
			}
	
			try {
							
				// collect all variables using any possible getter method on the class
				for (Method m : c.getDeclaredMethods()) {
					if (m.getName().toLowerCase().startsWith("get")) {
						// found a class setter, get the value now
						ret.put(m.getName().toLowerCase().substring(3), RunRules.convert(m.invoke(o)));
					}
				}
				
				// this is to collect any output variables added in by Signavio with _Output in the name
				// output variables can also be added directly to the data object itself
				// that is read and manipulated by the drools rules themselves
				ArrayList<Object> outputs = re.objectsFind("_Output");
				if(outputs != null) {
					for(Object output : outputs) {
						Class oc = output.getClass();
						
						for (Method m : oc.getDeclaredMethods()) {
							if (m.getName().toLowerCase().startsWith("get")) {
								// found a class setter, get the value now
								Object retout = m.invoke(output);
								if(retout != null) {
									String outputlabel = m.getName().toLowerCase().substring(3);
									if(m.getReturnType() == String.class) {
										ret.put("output_" + outputlabel, retout.toString());
										
									}else if(m.getReturnType() == BigDecimal.class) {
										ret.put("output_" + outputlabel, retout.toString());
										
									}else if(m.getReturnType() == List.class) {
										
										// array of outputs, need to enumerate
										outputlabel = "array";
										HashMap<String,String> outmsgs = new HashMap<String,String>();
										for(Object outitem : (List<Object>)retout) {
											Class ocitem = outitem.getClass();
											if(ocitem == String.class) {
												String mkey = m.getName().toLowerCase().substring(3);
												if(outmsgs.containsKey(mkey)) {
													outmsgs.replace(mkey, outmsgs.get(mkey) + "," + outitem.toString());
												}else {
													outmsgs.put(mkey, outitem.toString());
												}
											} else {										
												for (Method mitem : ocitem.getDeclaredMethods()) {
													if (mitem.getName().toLowerCase().startsWith("get")) {
														Object retoutitem = mitem.invoke(outitem);
														if(retoutitem != null) {
															String mkey = mitem.getName().toLowerCase().substring(3);
															if(outmsgs.containsKey(mkey)) {
																outmsgs.replace(mkey, outmsgs.get(mkey) + "," + retoutitem.toString());
															}else {
																outmsgs.put(mkey, retoutitem.toString());
															}
														}
													}
												}
											}
										}									
										for(Map.Entry<String,String> entry : outmsgs.entrySet()) {
											ret.put("output_" + entry.getKey(), entry.getValue());
										}
										
									}else if(m.getReturnType().getName().startsWith("com.signavio.droolsexport_")) {
										// this is for a return class object generated specifically by signavio 
										Class ocitem = retout.getClass();
										for (Method mitem : ocitem.getDeclaredMethods()) {
											if (mitem.getName().toLowerCase().startsWith("get")) {
												String olbl = mitem.getName().toLowerCase().substring(3);
												Object oval = mitem.invoke(retout);
												ret.put("output_" + olbl, oval.toString());
											}
										}
									}else {									
										try {
											ret.put("output_" + outputlabel, String.join(", ", (List<String>)retout));
										} catch (Exception e) {
											logger.error("Could not map output (not a List):" + e);
										}
									}																
									
								}
							}
						}
					}
				}
	
			} catch (Exception e) {
				logger.error("Could not invoke getters :" + e);
			}	
			
		} catch (Exception e) {
			logger.error("EXCEPTION running RunRules :" + e);
		}	
		
		return ret;
	}

}
