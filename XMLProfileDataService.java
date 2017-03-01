/**
# *******************************************************************************
#  Licensed Materials - Property of IBM 
#  Unica Interact
#  (c) Copyright IBM Corporation 2001, 2012.
#  US Government Users Restricted Rights - Use, duplication or disclosure
#  restricted by GSA ADP Schedule Contract with IBM Corp. 
# *******************************************************************************
*/

package com.unicacorp.interact.samples.externalcallout;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

import com.unicacorp.interact.flowchart.macrolang.storedobjs.CalloutException;
import com.unicacorp.interact.session.AudienceId;
import com.unicacorp.interact.session.ISessionDataNonLeafNode;
import com.unicacorp.interact.session.ISessionDataRootNode;
import com.unicacorp.interact.session.SessionDataUtils;
import com.unicacorp.interact.session.profiledataservice.AudienceNotFoundException;
import com.unicacorp.interact.session.profiledataservice.IInteractProfileDataService;
import com.unicacorp.interact.treatment.optimization.IInteractSessionData;
import com.unicacorp.interact.treatment.optimization.InteractDataType;

/**
 * This class is an example implementation of IInteractProfileDataService.
 * It loads profile data from an XML file. An example of such file 
 * is included with this sample with the name SampleExternalData.xml.
 * 
 * It supports loading only a few hundred records from the XML file at the most.
 * 
 * To use this data service, go to the global configuration page,
 * under the node "Affinium|Interact|profile|Audience Levels|Customer",
 * click the link "(External Data Service)", then, type in a name for 
 * this service, type "com.unicacorp.interact.samples.externalcallout.XMLProfileDataService"
 * in className, then specify the classPath.
 * After that, click "(parameters)" to add the parameter(s) from the following list:
 * 1. fileLocation, which is mandatory. Its value is the absolute path to the XML 
 * file that has the profile data. The contents in that XML file must follow the format 
 * in the included SampleDataSource.xml file.
 * 2. dynamicLoading, which is optional. If it is "true", the profile data will be read 
 * from the specified XML file every time it is requested. This is a convenience for 
 * testing, since you can change the profile data on the fly and view the results without 
 * starting Interact run time. It its value is "false" or this attribute is not configured, 
 * the contents of the XML file will be loaded when Interact run time starts and will 
 * never refresh unless the run time is restarted.
 * 
 * In addition, in the design time, the audience level "customer" must be mapped
 * to a base table named customer_profile that include the following schema:
 * customerid int primary key,
 * name varchar not null,
 * age int not null
 * And the following 2 dimensional tables:
 * customer_email (customerid int not null, email varchar)
 * customer_address (customerid int not null, addressid int not null, address varchar)
 * 
 * NOTE: This class is meant to be a simple example showing how to implement IInteractProfileDataService.
 * It is not production ready and no technical support will be provided if used in a production environment.
 * 
 * @author ssu
 *
 */
public class XMLProfileDataService implements IInteractProfileDataService {

	private static String FILE_LOCATION = "fileLocation";
	private static String DYNAMIC_LOADING = "dynamicLoading";
	
	private Map<Double, CustomerRecord> customerMap;
	private String schema;
	private boolean dynamicLoading = false;
	
	public void initialize(Map<String, String> configurationData)
			throws CalloutException {
		String dynamicLoadingStr = configurationData.get(DYNAMIC_LOADING);
		if (dynamicLoadingStr != null) {
			dynamicLoading = Boolean.parseBoolean(dynamicLoadingStr);
		}
		
		if (!dynamicLoading) {
			loadProfile(configurationData);
		}
	}
	
	private void loadProfile(Map<String, String> configurationData) {
		customerMap = new HashMap<Double, CustomerRecord>();
		
		String fileLocation = configurationData.get(FILE_LOCATION);
		File dataFile = new File(fileLocation);
		NodeList recordList;
		try {
			DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document doc = builder.parse(dataFile);
			
			Element rootElem = doc.getDocumentElement();
			schema = rootElem.getAttribute("schema");
			recordList = rootElem.getChildNodes();
			
			int recordCount = recordList.getLength();
			for (int idx=0; idx<recordCount; idx++) {
				Node node = recordList.item(idx);
				if (!node.getNodeName().equals("record")) {
					continue;
				}
				
				Element record = (Element) node;
				Double customerId = Double.valueOf(((Text)record.getElementsByTagName("customerid").item(0).getFirstChild()).getData());
				String name = ((Text)record.getElementsByTagName("name").item(0).getFirstChild()).getData();
				Double age = Double.valueOf(((Text)record.getElementsByTagName("age").item(0).getFirstChild()).getData());
				CustomerRecord customer = new CustomerRecord(customerId, name, age);
				customerMap.put(Double.valueOf((double) customerId), customer);
			
				NodeList customerEmailListNode = record.getElementsByTagName("customer_email");
				if (customerEmailListNode.getLength() > 0) {
					NodeList emailRecordList = customerEmailListNode.item(0).getChildNodes();
					int emailCount = emailRecordList.getLength();
					for (int idxEmail=0; idxEmail<emailCount; idxEmail++) {
						Node emailRecordNode = emailRecordList.item(idxEmail);
						if (!emailRecordNode.getNodeName().equals("record")) {
							continue;
						}
						
						Element emailRecord = (Element) emailRecordNode;
						String email = ((Text) emailRecord.getElementsByTagName("email").item(0).getFirstChild()).getData();
						customer.addEmail(email);
					}
				}

				NodeList customerAddressListNode = record.getElementsByTagName("customer_address");
				if (customerAddressListNode.getLength() > 0) {
					NodeList addressRecordList = customerAddressListNode.item(0).getChildNodes();
					int addressCount = addressRecordList.getLength();
					for (int idxAddress=0; idxAddress<addressCount; idxAddress++) {
						Node addressRecordNode = addressRecordList.item(idxAddress);
						if (!addressRecordNode.getNodeName().equals("record")) {
							continue;
						}
						
						Element addressRecord = (Element) addressRecordNode;
						Double addressId = Double.valueOf(((Text) addressRecord.getElementsByTagName("addressid").item(0).getFirstChild()).getData());
						String address = ((Text) addressRecord.getElementsByTagName("address").item(0).getFirstChild()).getData();
						customer.addAddress(addressId, address);
					}
				}
			}
		}
		catch (Exception e) {
			throw new com.unica.campaign.core.exceptions.SystemException("Exception loading data from file " + fileLocation, e);
		}
	}

	public void shutdown(Map<String, String> configurationData)
			throws CalloutException {
		if (customerMap != null) {
			customerMap.clear();
		}
	}

	public ISessionDataRootNode loadData(AudienceId audienceId,
			IInteractSessionData sessionData, Map<String, String> configurationData)
			throws CalloutException, AudienceNotFoundException {
		if (dynamicLoading) {
			loadProfile(configurationData);
		}
		
		CustomerRecord record = customerMap.get((Double) audienceId.getComponentValue("customerid"));
		if (record == null) {
			throw new AudienceNotFoundException();
		}
		
		ISessionDataRootNode data = createSessionData(record);
		
		return data;
	}
	
	private ISessionDataRootNode createSessionData(CustomerRecord customer) {
		
		if (customer == null) {
			return null;
		}
		
		ISessionDataRootNode root = SessionDataUtils.createRootNode("customer_profile");
		
		SessionDataUtils.createLeafNode(root, "customerid", InteractDataType.String, Arrays.asList((Object) customer.getId()));
		SessionDataUtils.createLeafNode(root, "name", InteractDataType.String, Arrays.asList((Object) customer.getName()));
		SessionDataUtils.createLeafNode(root, "age", InteractDataType.Double, Arrays.asList((Object) customer.getAge()));
		
		List<String> emailList = customer.getEmailList();
		if (emailList != null) {
			ISessionDataNonLeafNode emailListNode = SessionDataUtils.createNonLeafNode(root, schema + "_customer_email");
			SessionDataUtils.createLeafNode(emailListNode, "email", null, emailList);
		}
		
		List<CustomerAddressRecord> addressRecordList = customer.getAddressList();
		if (addressRecordList != null) {
			ISessionDataNonLeafNode addressListNode = SessionDataUtils.createNonLeafNode(root, schema + "_customer_address");

			List<Double> addressIdLst = new ArrayList<Double>(addressRecordList.size());
			List<String> addressLst = new ArrayList<String>(addressRecordList.size());
			for (CustomerAddressRecord address : addressRecordList) {
				addressIdLst.add(address.getAddressId());
				addressLst.add(address.getAddress());
			}
			SessionDataUtils.createLeafNode(addressListNode, "addressid", null, addressIdLst);
			SessionDataUtils.createLeafNode(addressListNode, "address", null, addressLst);
		}
		
		return root;
	}
}