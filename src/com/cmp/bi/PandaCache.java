package com.cmp.bi;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.couchbase.client.protocol.views.View;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;

public class PandaCache {

	public static CouchbaseClient couchClient = null;
	public static String designDoc = "pandalytics";

	public PandaCache() {
		if (couchClient == null) {
			LinkedList<URI> uris = new LinkedList();

			uris.add(URI.create("http://127.0.0.1:8091/pools"));
			initCacheClient(uris);
		}
	}

	public PandaCache(LinkedList<URI> uris) {
		if (couchClient == null) {
			initCacheClient(uris);
		}
	}

	public void initCacheClient(LinkedList<URI> uris) {
		try {
			CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
			cfb.setOpTimeout(10000L);

			couchClient = new CouchbaseClient(cfb.buildCouchbaseConnection(uris, "default", ""));
			System.out.println("************************************* Initialised CouchClient ********** " + couchClient.toString());
		} catch (IOException e) {
			System.err.println("IOException connecting to Panda Cache: " + e.getMessage());
			System.exit(1);
		}
	}

	public CouchbaseClient getClient() {
		return couchClient;
	}

	public View getView(String viewName) {
		return couchClient.getView(designDoc, viewName);
	}

	public void set(String id, int expiry, String data) {
		try {
			couchClient.set(id, expiry, data);
		} catch (Exception e) {
			System.out.println("Node Down? Exception : " + e.toString());
		}
	}

	public void delete(String id) {
		try {
			couchClient.delete(id);
		} catch (Exception e) {
			System.out.println("Node Down? Exception : " + e.toString());
		}
	}

	public Object get(String id) {
		Object result = null;
		try {
			result = getClient().get(id);
		} catch (Exception e) {
			System.out.println("Node down? Will try to get from Replica : "
					+ e.toString());
			try {
				//result = couchClient.getFromReplica(id);
			} catch (Exception eRep) {
				System.out.println("Getting from Replica Failed. May be no replica available : " + eRep.toString());
				return null;
			}
		}
		return result;
	}
}
