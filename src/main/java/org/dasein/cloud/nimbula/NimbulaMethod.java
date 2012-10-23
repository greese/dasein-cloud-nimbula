/**
 * Copyright (C) 2009-2012 enStratus Networks Inc
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.cloud.nimbula;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.util.EasySsl;
import org.json.JSONException;
import org.json.JSONObject;

public class NimbulaMethod {
    //static private final Logger logger     = NimbulaDirector.getLogger(NimbulaMethod.class);
    static private final Logger wireLogger = NimbulaDirector.getWireLogger(NimbulaMethod.class);
    
    private String             authCookie  = null;
    private NimbulaDirector    cloud       = null;
    private String             response    = null;
    private String             url         = null;
    
    public NimbulaMethod(@Nonnull NimbulaDirector cloud, @Nonnull String resource) throws CloudException {
        super();
        ProviderContext ctx = cloud.getContext();
        
        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }

        Properties properties = ctx.getCustomProperties();
        String ignoreCertSignature = null;

        if( properties != null ) {
            ignoreCertSignature = properties.getProperty("ignoreCertSignature", "false");
        }
        if( ignoreCertSignature != null && ignoreCertSignature.equalsIgnoreCase("true") ) {
            ProtocolSocketFactory sf = new EasySsl();
            Protocol easyhttps = new Protocol("https", sf, 443 );
            Protocol.registerProtocol( "https", easyhttps );
        }
        this.cloud = cloud;
        url = cloud.getURL(resource);
        if( properties != null ) {
            authCookie = properties.getProperty("nimbulaAuthCookie");
        }
        if( authCookie != null && authCookie.trim().length() < 1 ) {
            authCookie = null;
        }
    }

    private void authenticate() throws IOException, CloudException, InternalException {
        if( authCookie != null ) {
            return;
        }
        ProviderContext ctx = cloud.getContext();

        if( ctx == null ) {
            throw new CloudException("Unable to authenticate without a context");
        }
        HttpClient client = getClient();
        PostMethod post = new PostMethod(cloud.getURL("authenticate") + "/");
        HashMap<String,Object> request = new HashMap<String,Object>();

        request.put("user", "/" + ctx.getAccountNumber() + "/" + new String(ctx.getAccessPublic(), "utf-8"));
        request.put("password", new String(ctx.getAccessPrivate(), "utf-8"));
        post.addRequestHeader("Accept", "application/nimbula-v2+json");
        post.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
        post.setRequestEntity(new StringRequestEntity((new JSONObject(request)).toString(), "application/nimbula-v2+json", "UTF-8"));
        if( wireLogger.isDebugEnabled() ) {
            wireLogger.debug("POST " + post.getPath());
            for( Header header : post.getRequestHeaders() ) {
                wireLogger.debug(header.getName() + ": " + header.getValue());
            }
            String body = ((new JSONObject(request)).toString());
            String[] lines = body.split("\n");
            
            if( lines == null || lines.length < 1 ) {
                lines = new String[] { body };
            }
            for( String line : lines ) {
                wireLogger.debug(" -> " + line.trim());
            }
        }
        int code = client.executeMethod(post);
        
        wireLogger.debug("HTTP STATUS: " + code);
        if( code != HttpServletResponse.SC_NO_CONTENT ) {
            response = post.getResponseBodyAsString();
            if( wireLogger.isDebugEnabled() ) {
                wireLogger.debug(response);
            }
        }
        checkResponse(post, code);
    }
    
    private void checkResponse(@Nonnull HttpMethod method, @Nonnegative int code) throws CloudException, InternalException {
        checkResponse(method, code, null);
    }
    
    private @Nonnull String getErrorMessage(@Nullable String body, @Nonnull String defaultMessage) {
        if( body != null ) {
            try {
                JSONObject ob = new JSONObject(body);
                String msg = ob.getString("message");
                
                if( ob.has("reference") ) {
                    String ref = ob.getString("reference");
                    
                    if( ref != null ) {
                        msg = msg + " [reference=" + ref + "]";
                    }
                }
                return msg;
            }
            catch( Throwable ignore ) {
                // ignore
            }
        }
        return defaultMessage;
    }
    
    private void checkResponse(@Nonnull HttpMethod method, @Nonnegative int code, @Nullable String responseBody) throws CloudException, InternalException {
        ProviderContext ctx = cloud.getContext();
        
        if( ctx == null ) {
            throw new CloudException("No context is set for this request");
        }
        Header[] headers = method.getResponseHeaders("Set-Cookie");

        if( headers != null ) {
            for( Header header : headers ) {
                if( header.getValue().startsWith("nimbula=") ) {
                    authCookie = header.getValue();

                    Properties props = ctx.getCustomProperties();

                    if( props == null ) {
                        props = new Properties();
                        ctx.setCustomProperties(props);
                    }
                    props.setProperty("nimbulaAuthCookie", authCookie);
                }
            }
        }
        String message;
        
        switch( code ) {
            case 401:
                message = getErrorMessage(responseBody, "You must authenticate before making this call");
                throw new InternalException(code + ": " + message);
            case 403: 
                message = getErrorMessage(responseBody, "You do not have access to the requested resource");
                throw new CloudException(code + ": " + message);
            case 404: return;
            case 405:
                message = getErrorMessage(responseBody, "Invalid HTTP method");
                throw new InternalException(code + ": " + message);
            case 406:
                message = getErrorMessage(responseBody, "Invalid request for resource");
                throw new InternalException(code + ": " + message);
            case 409: 
                message = getErrorMessage(responseBody, "A conflict exists with the resource you were accessing");
                throw new CloudException(code + ": " + message);
            case 410: 
                message = getErrorMessage(responseBody, "The resource you are referencing no longer exists");
                throw new CloudException(code + ": " + message);
            case 415:
                message = getErrorMessage(responseBody, "Request is not JSON");
                throw new InternalException(code + ": " + message);                
            default: 
                if( code >= 500 ) {
                    message = getErrorMessage(responseBody, "Unknown error");
                    throw new CloudException(code + ": " + message);
                }
        }
    }
    
    public @Nonnegative int delete(@Nonnull String target) throws IOException, CloudException, InternalException {
        authenticate();
        
        HttpClient client = getClient();
        DeleteMethod delete = new DeleteMethod(getUrl(url, target));

        delete.addRequestHeader("Accept", "application/nimbula-v2+json");
        delete.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
        delete.setRequestHeader("Cookie", authCookie);
        if( wireLogger.isDebugEnabled() ) {
            wireLogger.debug("DELETE " + delete.getPath());
            for( Header header : delete.getRequestHeaders() ) {
                wireLogger.debug(header.getName() + ": " + header.getValue());
            }
        }
        int code = client.executeMethod(delete);
        
        wireLogger.debug("HTTP STATUS: " + code);
        if( code != HttpServletResponse.SC_NO_CONTENT ) {
            response = delete.getResponseBodyAsString();
            if( wireLogger.isDebugEnabled() ) {
                wireLogger.debug(response);
            }
        }
        checkResponse(delete, code);
        return code;
    }
    
    public @Nonnegative int get(@Nonnull String target) throws IOException, CloudException, InternalException {
        authenticate();
        
        HttpClient client = getClient();
        if( !target.startsWith("/") ) {
            target = getUrl(url, target);
        }
        else {
            target = url + target;
        }
        GetMethod get = new GetMethod(target);

        get.addRequestHeader("Accept", "application/nimbula-v2+json");
        get.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
        get.setRequestHeader("Cookie", authCookie);
        if( wireLogger.isDebugEnabled() ) {
            wireLogger.debug("GET " + get.getPath());
            for( Header header : get.getRequestHeaders() ) {
                wireLogger.debug(header.getName() + ": " + header.getValue());
            }
        }
        int code = client.executeMethod(get);

        wireLogger.debug("HTTP STATUS: " + code);
        if( code == 401 ) {
            return code;
        }
        if( code != HttpServletResponse.SC_NO_CONTENT ) {
            response = get.getResponseBodyAsString();
            if( wireLogger.isDebugEnabled() ) {
                wireLogger.debug(response);
            }
        }
        checkResponse(get, code);
        return code;
    }   
    
    private @Nonnull HttpClient getClient() {
        ProviderContext ctx = cloud.getContext();
        HttpClient client = new HttpClient();

        if( ctx != null ) {
            Properties p = ctx.getCustomProperties();

            if( p != null ) {
                String proxyHost = p.getProperty("proxyHost");
                String proxyPort = p.getProperty("proxyPort");

                if( proxyHost != null ) {
                    int port = 0;

                    if( proxyPort != null && proxyPort.length() > 0 ) {
                        port = Integer.parseInt(proxyPort);
                    }
                    client.getHostConfiguration().setProxy(proxyHost, port);
                }
            }
        }
        return client;
    }
    
    public @Nonnull JSONObject getResponseBody() throws JSONException {
        return new JSONObject(response);
    }
    
    private @Nonnull String getUrl(@Nonnull String endpoint, @Nullable String id) throws CloudException, InternalException {
        ProviderContext ctx = cloud.getContext();
        
        if( ctx == null ) {
            throw new CloudException("No context is set for this request");
        }
        try {
            String user = new String(ctx.getAccessPublic(), "utf-8");
            String account = ctx.getAccountNumber();
            
            if( !endpoint.endsWith("/") ) {
                if( id != null && id.startsWith("/") ) {
                    return endpoint + id;
                }
                else if( id != null ) {
                    return endpoint + "/" + account + "/" + user + "/" + id;
                }
                else {
                    return endpoint + "/" + account + "/" + user;
                }
            }
            else if( id != null && id.startsWith("/") ) {
                if( id.equals("/") ) {
                    return endpoint;
                }
                else {
                    return endpoint + id.substring(1);
                }
            }
            else if( id != null ) {
                return endpoint + id;
            }
            else {
                return endpoint + account + "/" + user;
            }
        }
        catch( UnsupportedEncodingException e ) {
            throw new InternalException(e);
        }
    }
    
    public @Nonnegative int list() throws IOException, CloudException, InternalException {
        ProviderContext ctx = cloud.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        authenticate();
        
        HttpClient client = getClient();
        GetMethod get;
        
        if( url.endsWith("info") ) {
            get = new GetMethod(url + "/");
        }
        else {
            get = new GetMethod(url + "/" + ctx.getAccountNumber() + "/");
        }
        get.addRequestHeader("Accept", "application/nimbula-v2+json");
        get.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
        get.setRequestHeader("Cookie", authCookie);
        if( wireLogger.isDebugEnabled() ) {
            wireLogger.debug("GET " + get.getPath());
            for( Header header : get.getRequestHeaders() ) {
                wireLogger.debug(header.getName() + ": " + header.getValue());
            }
        }
        int code = client.executeMethod(get);

        wireLogger.debug("HTTP STATUS: " + code);
        if( code != HttpServletResponse.SC_NO_CONTENT ) {
            response = get.getResponseBodyAsString();
            if( wireLogger.isDebugEnabled() ) {
                wireLogger.debug(response);
            }
        }
        checkResponse(get, code);
        return code;        
    }
    
    @SuppressWarnings("unused")
    public @Nonnegative int discover() throws IOException, CloudException, InternalException {
        return discover(null);
    }
    
    public @Nonnegative int discover(@Nullable String userId) throws IOException, CloudException, InternalException {
        ProviderContext ctx = cloud.getContext();
        
        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        authenticate();
        String target = "/" + ctx.getAccountNumber() + "/";
        
        if( userId != null ) {
            target = target + userId + "/";
        }
        HttpClient client = getClient();
        GetMethod get = new GetMethod(url + target);

        get.addRequestHeader("Accept", "application/nimbula-v2+directory+json");
        get.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
        get.setRequestHeader("Cookie", authCookie);
        if( wireLogger.isDebugEnabled() ) {
            wireLogger.debug("GET " + get.getPath());
            for( Header header : get.getRequestHeaders() ) {
                wireLogger.debug(header.getName() + ": " + header.getValue());
            }
        }
        int code = client.executeMethod(get);

        wireLogger.debug("HTTP STATUS: " + code);
        if( code != HttpServletResponse.SC_NO_CONTENT ) {
            response = get.getResponseBodyAsString();
            if( wireLogger.isDebugEnabled() ) {
                wireLogger.debug(response);
            }
        }
        checkResponse(get, code);
        return code;        
    }
    
    public @Nonnegative int post(@Nonnull Map<String,Object> state) throws IOException, CloudException, InternalException {
        authenticate();
        HttpClient client = getClient();
        PostMethod post = new PostMethod(url + "/");

        post.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
        post.setRequestHeader("Cookie", authCookie);
        post.addRequestHeader("Accept", "application/nimbula-v2+json");
        post.setRequestEntity(new StringRequestEntity((new JSONObject(state)).toString(), "application/nimbula-v2+json", "UTF-8"));
        if( wireLogger.isDebugEnabled() ) {
            wireLogger.debug("POST " + post.getPath());
            for( Header header : post.getRequestHeaders() ) {
                wireLogger.debug(header.getName() + ": " + header.getValue());
            }
            String body = ((new JSONObject(state)).toString());
            String[] lines = body.split("\n");
            
            if( lines == null || lines.length < 1 ) {
                lines = new String[] { body };
            }
            for( String line : lines ) {
                wireLogger.debug(" -> " + line.trim());
            }
        }
        int code = client.executeMethod(post);
        
        wireLogger.debug("HTTP STATUS: " + code);
        if( code != HttpServletResponse.SC_NO_CONTENT ) {
            response = post.getResponseBodyAsString();
            if( wireLogger.isDebugEnabled() ) {
                wireLogger.debug(response);
            }
            checkResponse(post, code, response);
        }
        else {
            checkResponse(post, code);
        }
        return code;
    }

    @SuppressWarnings("unused")
    public @Nonnegative int put(@Nonnull String targetId, @Nonnull Map<String,Object> state) throws IOException, CloudException, InternalException {
        authenticate();
        HttpClient client = getClient();
        PutMethod put = new PutMethod(getUrl(url, targetId));

        put.addRequestHeader("Content-Type", "application/json");
        put.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
        put.setRequestHeader("Cookie", authCookie);
        put.setRequestEntity(new StringRequestEntity((new JSONObject(state)).toString(), "application/json", "UTF-8"));
        if( wireLogger.isDebugEnabled() ) {
            wireLogger.debug("PUT " + put.getPath());
            for( Header header : put.getRequestHeaders() ) {
                wireLogger.debug(header.getName() + ": " + header.getValue());
            }
            String body = ((new JSONObject(state)).toString());
            String[] lines = body.split("\n");
            
            if( lines == null || lines.length < 1 ) {
                lines = new String[] { body };
            }
            for( String line : lines ) {
                wireLogger.debug(" -> " + line.trim());
            }
        }
        int code = client.executeMethod(put);
        
        wireLogger.debug("HTTP STATUS: " + code);
        if( code != HttpServletResponse.SC_NO_CONTENT ) {
            response = put.getResponseBodyAsString();
            if( wireLogger.isDebugEnabled() ) {
                wireLogger.debug(response);
            }
        }
        checkResponse(put, code);
        return code;
    }
}