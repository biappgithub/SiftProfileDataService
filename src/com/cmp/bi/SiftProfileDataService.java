package com.cmp.bi;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.renjin.sexp.SEXP;

import com.cmp.bi.PandaCache;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonElement;
import com.unicacorp.interact.flowchart.macrolang.storedobjs.CalloutException;
import com.unicacorp.interact.session.AudienceId;
import com.unicacorp.interact.session.ISessionDataRootNode;
import com.unicacorp.interact.session.profiledataservice.AudienceNotFoundException;
import com.unicacorp.interact.session.profiledataservice.IInteractProfileDataService;
import com.unicacorp.interact.treatment.optimization.IInteractSessionData;

public class SiftProfileDataService implements IInteractProfileDataService {
	
	public static HashMap<String, JsonObject> rScriptsMap;
	public static HashMap<String, String> rfilesContentMap;
	protected static ScriptEngineManager scriptEngineManager = null;
	protected static ScriptEngine scriptEngine = null;
	private static Logger logger = null;
	long uniqMSISDNS = 0L;
	long numRecords = 0L;
	long idleFoks = 0L;
	protected static PandaCache cache = null;
	protected static JsonParser parser = null;
	
	public void initialize(Map<String, String> arg0) throws CalloutException {
		logger = Logger.getLogger(SiftProfileDataService.class);			
		logger.info("=== Start Call initialize ===");	
		/*
		 runtime environment reserved parameters
			 UACIEventID
			 UACIEventName
			 UACIInteractiveChannelID
			 UACIInteractiveChannelName
			 UACIInteractionPointID
			 UACIInteractionPointName
			 UACISessionID		 
		 */
	}

	public ISessionDataRootNode loadData(AudienceId audienceId,IInteractSessionData sessionData, Map<String, String> map) throws CalloutException, AudienceNotFoundException {
		logger.info("=== Start Call ISessionDataRootNode ===" + sessionData.getValue("UACIInteractiveChannelName"));			
		//String addrPersist = "http://10.89.104.103:8091/pools,http://10.89.104.104:8091/pools";	
		//String jsonFileName = "/opt/IBM/Unica/Interact/batchProcess/rscripts/WoW.R.json";
		ResultSet sqlResult = null; 
		String addrPersist = (String)map.get("addrPersist");
		String[] addrs = addrPersist.split(",");	
		String jsonFileName = (String)map.get("addrJson");
		
		sessionData.getValue("UACIInteractiveChannelName");
		logger.info("=== Start Connect Database ===");
		try {
			Context ctx = new InitialContext();
			DataSource dataSource = (DataSource) ctx.lookup("CheckProfileDNS");
			Connection conn = dataSource.getConnection();
			Statement select = conn.createStatement();
			sqlResult = select.executeQuery("select name from usm_user");
    		while (sqlResult.next()) {
    			logger.info("=== Fetch property1 ===" + sqlResult.getString("name"));
    			
    		}
			// use the connection
			conn.close();
		} catch (Exception e) {
			logger.info("=== Connect Database ===" + e.getMessage());
		}
		
		logger.info("=== Check Parameter addrPersist ===" + addrPersist);
		logger.info("=== Check Parameter addrJson ===" + jsonFileName);
		logger.info("=== getComponentValue ===" + audienceId.getComponentValue(audienceId.getAudienceLevel()));
		
		try {
			initializeParam(addrs);						
		} catch (Exception e) {
			logger.error("=== initializeParam ===" + e.fillInStackTrace());
			e.printStackTrace();
		}

		JsonObject subscriberIndicators = null;
		JsonParser parser = new JsonParser();
		String strIndicators = (String) cache.getClient().get("66800000999" + "-Indicators");
		subscriberIndicators = (JsonObject) parser.parse(strIndicators);	
				
		JsonObject scriptDef = null;	    
	    JsonObject jResults = new JsonObject();
		try {
			if (rScriptsMap != null) {
				scriptDef = (JsonObject)rScriptsMap.get(jsonFileName);
			}
			if (scriptDef == null){
				String strScriptDef = readFile(jsonFileName);
				scriptDef = (JsonObject)new JsonParser().parse(strScriptDef);
				rScriptsMap = new HashMap();
				rScriptsMap.put(jsonFileName, scriptDef);
			}
			
			if (scriptEngineManager == null) {
				scriptEngineManager = new ScriptEngineManager();
			}
			if (scriptEngine == null) {
				scriptEngine = scriptEngineManager.getEngineByName("Renjin");
			}
			if (scriptEngine == null) {
				throw new RuntimeException("Renjin Script Engine not found on the classpath.");
			}
			
			JsonArray arrParamNames = scriptDef.get("paramNames").getAsJsonArray();
			JsonArray paramTypes = scriptDef.get("paramTypes").getAsJsonArray();
			JsonArray arrSiftParamNames = scriptDef.get("siftParamMap").getAsJsonArray();
			Object[] parameterValues = new Object[arrParamNames.size()];
			
			for (int j = 0; j < arrSiftParamNames.size(); j++){
				String thisParam = arrSiftParamNames.get(j).getAsString();
				String thisParamType = paramTypes.get(j).getAsString();				
		        Object jValue = null;		        
		        jValue = subscriberIndicators.get(thisParam);		        
				parameterValues[j] = createObject(thisParamType,jValue);									
			}
			
			for (int i = 0; i < arrParamNames.size(); i++) {
				String setParamName = arrParamNames.get(i).getAsString();
				scriptEngine.put(setParamName, parameterValues[i]);
			}
						
			String fileName = scriptDef.get("RscriptPath").getAsString();
			String content = null;
			
			if (rfilesContentMap != null) {
				content = (String)rfilesContentMap.get(fileName);
			}
			if (content == null){
				content = readFile(fileName);
		        rfilesContentMap = new HashMap();
		        rfilesContentMap.put(fileName, content);
			}
			SEXP result = (SEXP)scriptEngine.eval(content);
			
		} catch (Exception e) {
			logger.error("=== EXEC R Script ===" + e.fillInStackTrace());
			SEXP result;
			//return new JsonObject();
		}
		
		SEXP result;
	    JsonArray arrResults = scriptDef.get("returnValues").getAsJsonArray();
	    JsonArray arrResultTypes = scriptDef.get("returnTypes").getAsJsonArray();
	    JsonArray arrprofileName = scriptDef.get("profileName").getAsJsonArray();
	    for (int i = 0; i < arrResults.size(); i++){
	    	String retName = arrResults.get(i).getAsString();
	    	String profileName = arrprofileName.get(i).getAsString();
	    	SEXP sexpVal = (SEXP)scriptEngine.get(retName);
	    	String retType = arrResultTypes.get(i).getAsString();
	    	if (retType.equals("double")) {
	    		sessionData.setValue(profileName, Double.valueOf(sexpVal.asReal()));
	    		logger.info("=== Map Model with Profile ===" + profileName + "->" + Double.valueOf(sexpVal.asReal()));
	    	} else if (retType.equals("String")) {
	    		sessionData.setValue(profileName, sexpVal.asString());
	    		logger.info("=== Map Model with Profile ===" + profileName + "->" + sexpVal.asString());
	      }
	    }
		return null;
	}

	public void shutdown(Map<String, String> map) throws CalloutException {
		logger.info("=== Start Call shutdown ===");
		
	}
	
	public static void initializeParam(String[] strPersistAddress) throws Exception {
		LinkedList<URI> uris = new LinkedList();
		for (int i = 0; i < strPersistAddress.length; i++) {
			uris.add(new URI(strPersistAddress[i]));
		}
		cache = new PandaCache(uris);	
		parser = new JsonParser();
				
	}
	
	public static Object createObject(String type, Object jvalue) throws Exception{
		if(type.equals("double")){
			if (jvalue == null) {
				return Double.class.getConstructor(new Class[] { String.class }).newInstance(new Object[] { "0" });
			}
			return Double.class.getConstructor(new Class[] { String.class }).newInstance(new Object[] { ((JsonElement)jvalue).getAsString() });		}
		if(type.equals("String")){
			if (jvalue == null) {
				return new String();
			}
			return ((JsonElement)jvalue).getAsString();
		}
		if(type.equals("double[]")){
			if (jvalue == null) {
				return new double[0];
			}
			JsonArray jArr = ((JsonElement)jvalue).getAsJsonArray();
			double[] res = new double[jArr.size()];
			for (int i = 0; i < jArr.size(); i++) {
				res[i] = jArr.get(i).getAsDouble();
			}
			return res;
		}
		return jvalue;
		
	}
	
	 static String readFile(String fileName) throws IOException
	  {
	    BufferedReader br = new BufferedReader(new FileReader(fileName));
	    try
	    {
	      StringBuilder sb = new StringBuilder();
	      String line = br.readLine();
	      while (line != null)
	      {
	        sb.append(line);
	        sb.append("\n");
	        line = br.readLine();
	      }
	      return sb.toString();
	    }
	    finally
	    {
	      br.close();
	    }
	  }
	 
	public static void main(String[] args) throws Exception {
		String addrPersist = "http://10.89.104.103:8091/pools,http://10.89.104.104:8091/pools";	
		String[] addrs = addrPersist.split(",");	
		String jsonFileName = "WoW.R.JSON";
		try {
			initializeParam(addrs);
		} catch (Exception e) {
			e.printStackTrace();
		}
		JsonObject subscriberIndicators = null;
		JsonParser parser = new JsonParser();
		String strIndicators = (String) cache.getClient().get("66800000999" + "-Indicators");
		subscriberIndicators = (JsonObject) parser.parse(strIndicators);
						
		JsonObject scriptDef = null;	    
	    JsonObject jResults = new JsonObject();
		try {
			if (rScriptsMap != null) {
				scriptDef = (JsonObject)rScriptsMap.get(jsonFileName);
			}
			if (scriptDef == null){
				String strScriptDef = readFile(jsonFileName);
				scriptDef = (JsonObject)new JsonParser().parse(strScriptDef);
				rScriptsMap = new HashMap();
				rScriptsMap.put(jsonFileName, scriptDef);
			}
			
			if (scriptEngineManager == null) {
				scriptEngineManager = new ScriptEngineManager();
			}
			if (scriptEngine == null) {
				scriptEngine = scriptEngineManager.getEngineByName("Renjin");
			}
			if (scriptEngine == null) {
				throw new RuntimeException("Renjin Script Engine not found on the classpath.");
			}
			
			JsonArray arrParamNames = scriptDef.get("paramNames").getAsJsonArray();
			JsonArray paramTypes = scriptDef.get("paramTypes").getAsJsonArray();
			JsonArray arrSiftParamNames = scriptDef.get("siftParamMap").getAsJsonArray();
			Object[] parameterValues = new Object[arrParamNames.size()];
			
			for (int j = 0; j < arrSiftParamNames.size(); j++){
				String thisParam = arrSiftParamNames.get(j).getAsString();
				String thisParamType = paramTypes.get(j).getAsString();
				System.out.println("=== Check Indicator Name ===" + thisParam);
				System.out.println("=== Checp Data Type ===" + thisParamType);
				
		        Object jValue = null;		        
		        jValue = subscriberIndicators.get(thisParam);		        
				parameterValues[j] = createObject(thisParamType,jValue);					
				
			}
			
			for (int i = 0; i < arrParamNames.size(); i++) {
				String setParamName = arrParamNames.get(i).getAsString();
				scriptEngine.put(setParamName, parameterValues[i]);
			}
						
			String fileName = scriptDef.get("RscriptPath").getAsString();
			String content = null;
			
			if (rfilesContentMap != null) {
				content = (String)rfilesContentMap.get(fileName);
			}
			if (content == null){
				content = readFile(fileName);
		        rfilesContentMap = new HashMap();
		        rfilesContentMap.put(fileName, content);
			}
			SEXP result = (SEXP)scriptEngine.eval(content);
			
		} catch (Exception e) {
			SEXP result;
			//return new JsonObject();
		}
		
		SEXP result;
	    JsonArray arrResults = scriptDef.get("returnValues").getAsJsonArray();
	    JsonArray arrResultTypes = scriptDef.get("returnTypes").getAsJsonArray();
	    JsonArray arrprofileName = scriptDef.get("profileName").getAsJsonArray();
	    for (int i = 0; i < arrResults.size(); i++){
	    	String retName = arrResults.get(i).getAsString();
	    	String profileName = arrprofileName.get(i).getAsString();
	    	SEXP sexpVal = (SEXP)scriptEngine.get(retName);
	    	String retType = arrResultTypes.get(i).getAsString();
	    	if (retType.equals("double")) {
	    		jResults.addProperty(profileName, Double.valueOf(sexpVal.asReal()));
	    		
	    		
	    	} else if (retType.equals("String")) {
	    		jResults.addProperty(profileName, sexpVal.asString());
	      }
	    }
	    System.out.println("=== Check Result ===" + jResults);
	}
	
	
}
