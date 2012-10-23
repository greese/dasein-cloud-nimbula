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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.json.JSONException;
import org.json.JSONObject;

public class NimbulaMethod {
    static private final Logger logger  = NimbulaDirector.getLogger(NimbulaMethod.class);
    static private final Logger wire    = NimbulaDirector.getWireLogger(NimbulaMethod.class);
    
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
            /*
            ProtocolSocketFactory sf = new EasySsl();
            Protocol easyhttps = new Protocol("https", sf, 443 );
            Protocol.registerProtocol( "https", easyhttps );
            */
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

    private @Nonnull HttpClient getClient(@Nonnull ProviderContext ctx, boolean ssl) {
        HttpParams params = new BasicHttpParams();

        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        //noinspection deprecation
        HttpProtocolParams.setContentCharset(params, HTTP.UTF_8);
        HttpProtocolParams.setUserAgent(params, "Dasein Cloud");

        Properties p = ctx.getCustomProperties();

        if( p != null ) {
            String proxyHost = p.getProperty("proxyHost");
            String proxyPort = p.getProperty("proxyPort");

            if( proxyHost != null ) {
                int port = 0;

                if( proxyPort != null && proxyPort.length() > 0 ) {
                    port = Integer.parseInt(proxyPort);
                }
                params.setParameter(ConnRoutePNames.DEFAULT_PROXY, new HttpHost(proxyHost, port, ssl ? "https" : "http"));
            }
        }
        return new DefaultHttpClient(params);
    }

    private void authenticate() throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + NimbulaMethod.class.getName() + ".authenticate()");
        }
        try {
            if( authCookie != null ) {
                return;
            }
            String uri = cloud.getURL("authenticate") + "/";

            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug(">>> [POST (" + (new Date()) + ")] -> " + uri + " >--------------------------------------------------------------------------------------");
            }
            try {
                ProviderContext ctx = cloud.getContext();

                if( ctx == null ) {
                    throw new CloudException("Unable to authenticate without a context");
                }
                HttpClient client = getClient(ctx, uri.startsWith("https"));
                HttpPost post = new HttpPost(uri);
                HashMap<String,Object> request = new HashMap<String,Object>();

                try {
                    request.put("user", "/" + ctx.getAccountNumber() + "/" + new String(ctx.getAccessPublic(), "utf-8"));
                    request.put("password", new String(ctx.getAccessPrivate(), "utf-8"));
                }
                catch( UnsupportedEncodingException e ) {
                    throw new InternalException(e);
                }
                post.addHeader("Accept", "application/nimbula-v2+json");

                try {
                    //noinspection deprecation
                    post.setEntity(new StringEntity((new JSONObject(request)).toString(), "application/nimbula-v2+json", "UTF-8"));
                }
                catch( UnsupportedEncodingException e ) {
                    throw new InternalException(e);
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(post.getRequestLine().toString());
                    for( Header header : post.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");

                    try { wire.debug(EntityUtils.toString(post.getEntity())); }
                    catch( IOException ignore ) { }

                    wire.debug("");
                }
                HttpResponse response;

                try {
                    response = client.execute(post);
                    if( wire.isDebugEnabled() ) {
                        wire.debug(response.getStatusLine().toString());
                        for( Header header : response.getAllHeaders() ) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                    }
                }
                catch( IOException e ) {
                    logger.error("I/O error from server communications: " + e.getMessage());
                    e.printStackTrace();
                    throw new InternalException(e);
                }
                int code = response.getStatusLine().getStatusCode();

                logger.debug("HTTP STATUS: " + code);

                if( code != HttpServletResponse.SC_NO_CONTENT ) {
                    HttpEntity entity = response.getEntity();
                    String data = "";

                    if( entity != null ) {
                        try {
                            data = EntityUtils.toString(entity);
                        }
                        catch( IOException e ) {
                            throw new CloudException(e);
                        }
                    }
                    if( wire.isDebugEnabled() ) {
                        wire.debug(data);
                        wire.debug("");
                    }
                }
                checkResponse(response, code);
            }
            finally {
                if( wire.isDebugEnabled() ) {
                    wire.debug("<<< [POST (" + (new Date()) + ")] -> " + uri + " <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NimbulaMethod.class.getName() + ".authenticate()");
            }
        }
    }
    
    private void checkResponse(@Nonnull HttpResponse response, @Nonnegative int code) throws CloudException, InternalException {
        checkResponse(response, code, null);
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
    
    private void checkResponse(@Nonnull HttpResponse response, @Nonnegative int code, @Nullable String responseBody) throws CloudException, InternalException {
        ProviderContext ctx = cloud.getContext();
        
        if( ctx == null ) {
            throw new CloudException("No context is set for this request");
        }
        Header[] headers = response.getHeaders("Set-Cookie");

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
    
    public @Nonnegative int delete(@Nonnull String target) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + NimbulaMethod.class.getName() + ".delete(" + target + ")");
        }
        try {
            authenticate();
            String service = getUrl(url, target);

            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug(">>> [DELETE (" + (new Date()) + ")] -> " + service + " >--------------------------------------------------------------------------------------");
            }
            try {
                ProviderContext ctx = cloud.getContext();

                if( ctx == null ) {
                    throw new CloudException("No context was set for this request");
                }
                HttpClient client = getClient(ctx, service.startsWith("https"));
                HttpDelete delete = new HttpDelete(service);

                delete.addHeader("Accept", "application/nimbula-v2+json");
                delete.setHeader("Cookie", authCookie);
                if( wire.isDebugEnabled() ) {
                    wire.debug(delete.getRequestLine().toString());
                    for( Header header : delete.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
                HttpResponse response;

                try {
                    response = client.execute(delete);
                    if( wire.isDebugEnabled() ) {
                        wire.debug(response.getStatusLine().toString());
                        for( Header header : response.getAllHeaders() ) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                    }
                }
                catch( IOException e ) {
                    logger.error("I/O error from server communications: " + e.getMessage());
                    e.printStackTrace();
                    throw new InternalException(e);
                }
                int code = response.getStatusLine().getStatusCode();

                logger.debug("HTTP STATUS: " + code);
                checkResponse(response, code);
                return code;
            }
            finally {
                if( wire.isDebugEnabled() ) {
                    wire.debug("<<< [DELETE (" + (new Date()) + ")] -> " + service + " <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NimbulaMethod.class.getName() + ".delete()");
            }
        }
    }
    
    public @Nonnegative int get(@Nonnull String target) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + NimbulaMethod.class.getName() + ".get(" + target + ")");
        }
        try {
            authenticate();

            if( !target.startsWith("/") ) {
                target = getUrl(url, target);
            }
            else {
                target = url + target;
            }

            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug(">>> [GET (" + (new Date()) + ")] -> " + target + " >--------------------------------------------------------------------------------------");
            }
            try {
                ProviderContext ctx = cloud.getContext();

                if( ctx == null ) {
                    throw new CloudException("No context was set for this request");
                }
                HttpClient client = getClient(ctx, target.startsWith("https"));
                HttpGet get = new HttpGet(target);

                get.addHeader("Accept", "application/nimbula-v2+json");
                get.setHeader("Cookie", authCookie);

                if( wire.isDebugEnabled() ) {
                    wire.debug(get.getRequestLine().toString());
                    for( Header header : get.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
                HttpResponse response;

                try {
                    response = client.execute(get);
                    if( wire.isDebugEnabled() ) {
                        wire.debug(response.getStatusLine().toString());
                        for( Header header : response.getAllHeaders() ) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                    }
                }
                catch( IOException e ) {
                    logger.error("I/O error from server communications: " + e.getMessage());
                    e.printStackTrace();
                    throw new InternalException(e);
                }
                int code = response.getStatusLine().getStatusCode();

                logger.debug("HTTP STATUS: " + code);

                if( code == 401 ) {
                    return code;
                }
                if( code != HttpServletResponse.SC_NO_CONTENT ) {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        try {
                            this.response = EntityUtils.toString(entity);

                            if( wire.isDebugEnabled() ) {
                                wire.debug(this.response);
                                wire.debug("");
                            }
                        }
                        catch( IOException e ) {
                            throw new CloudException(e);
                        }
                    }
                }
                checkResponse(response, code);
                return code;
            }
            finally {
                if( wire.isDebugEnabled() ) {
                    wire.debug("<<< [GET (" + (new Date()) + ")] -> " + target + " <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NimbulaMethod.class.getName() + ".get()");
            }
        }
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
    
    public @Nonnegative int list() throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + NimbulaMethod.class.getName() + ".list()");
        }
        try {
            ProviderContext ctx = cloud.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            authenticate();

            String target;

            if( url.endsWith("info") ) {
                target = url + "/";
            }
            else {
                target = url + "/" + ctx.getAccountNumber() + "/";
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug(">>> [GET (" + (new Date()) + ")] -> " + target + " >--------------------------------------------------------------------------------------");
            }
            try {
                HttpClient client = getClient(ctx, target.startsWith("https"));
                HttpGet get = new HttpGet(target);

                get.addHeader("Accept", "application/nimbula-v2+json");
                get.setHeader("Cookie", authCookie);

                if( wire.isDebugEnabled() ) {
                    wire.debug(get.getRequestLine().toString());
                    for( Header header : get.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
                HttpResponse response;

                try {
                    response = client.execute(get);
                    if( wire.isDebugEnabled() ) {
                        wire.debug(response.getStatusLine().toString());
                        for( Header header : response.getAllHeaders() ) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                    }
                }
                catch( IOException e ) {
                    logger.error("I/O error from server communications: " + e.getMessage());
                    e.printStackTrace();
                    throw new InternalException(e);
                }
                int code = response.getStatusLine().getStatusCode();

                logger.debug("HTTP STATUS: " + code);

                if( code != HttpServletResponse.SC_NO_CONTENT ) {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        try {
                            this.response = EntityUtils.toString(entity);
                            if( wire.isDebugEnabled() ) {
                                wire.debug(this.response);
                                wire.debug("");
                            }
                        }
                        catch( IOException e ) {
                            throw new CloudException(e);
                        }
                    }
                }
                checkResponse(response, code);
                return code;
            }
            finally {
                if( wire.isDebugEnabled() ) {
                    wire.debug("<<< [GET (" + (new Date()) + ")] -> " + target + " <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NimbulaMethod.class.getName() + ".list()");
            }
        }
    }
    
    @SuppressWarnings("unused")
    public @Nonnegative int discover() throws CloudException, InternalException {
        return discover(null);
    }
    
    public @Nonnegative int discover(@Nullable String userId) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + NimbulaMethod.class.getName() + ".discover(" + userId + ")");
        }
        try {
            authenticate();

            ProviderContext ctx = cloud.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }

            String target = "/" + ctx.getAccountNumber() + "/";

            if( userId != null ) {
                target = target + userId + "/";
            }
            target = url + target;

            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug(">>> [GET (" + (new Date()) + ")] -> " + target + " >--------------------------------------------------------------------------------------");
            }
            try {
                HttpClient client = getClient(ctx, target.startsWith("https"));
                HttpGet get = new HttpGet(target);

                get.addHeader("Accept", "application/nimbula-v2+directory+json");
                get.setHeader("Cookie", authCookie);

                if( wire.isDebugEnabled() ) {
                    wire.debug(get.getRequestLine().toString());
                    for( Header header : get.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
                HttpResponse response;

                try {
                    response = client.execute(get);
                    if( wire.isDebugEnabled() ) {
                        wire.debug(response.getStatusLine().toString());
                        for( Header header : response.getAllHeaders() ) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                    }
                }
                catch( IOException e ) {
                    logger.error("I/O error from server communications: " + e.getMessage());
                    e.printStackTrace();
                    throw new InternalException(e);
                }
                int code = response.getStatusLine().getStatusCode();

                logger.debug("HTTP STATUS: " + code);

                if( code != HttpServletResponse.SC_NO_CONTENT ) {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        try {
                            this.response = EntityUtils.toString(entity);
                            if( wire.isDebugEnabled() ) {
                                wire.debug(this.response);
                                wire.debug("");
                            }
                        }
                        catch( IOException e ) {
                            throw new CloudException(e);
                        }
                    }
                }
                checkResponse(response, code);
                return code;
            }
            finally {
                if( wire.isDebugEnabled() ) {
                    wire.debug("<<< [GET (" + (new Date()) + ")] -> " + target + " <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NimbulaMethod.class.getName() + ".discover()");
            }
        }
    }
    
    public @Nonnegative int post(@Nonnull Map<String,Object> state) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + NimbulaMethod.class.getName() + ".post(" + state + ")");
        }
        try {
            authenticate();
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug(">>> [POST (" + (new Date()) + ")] -> " + url + "/ >--------------------------------------------------------------------------------------");
            }
            try {
                ProviderContext ctx = cloud.getContext();

                if( ctx == null ) {
                    throw new CloudException("No context was set for this request");
                }
                HttpClient client = getClient(ctx, url.startsWith("https"));
                HttpPost post = new HttpPost(url + "/");

                post.setHeader("Cookie", authCookie);
                post.addHeader("Accept", "application/nimbula-v2+json");
                try {
                    //noinspection deprecation
                    post.setEntity(new StringEntity((new JSONObject(state)).toString(), "application/nimbula-v2+json", "UTF-8"));
                }
                catch( UnsupportedEncodingException e ) {
                    throw new InternalException(e);
                }

                if( wire.isDebugEnabled() ) {
                    wire.debug(post.getRequestLine().toString());
                    for( Header header : post.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");

                    try { wire.debug(EntityUtils.toString(post.getEntity())); }
                    catch( IOException ignore ) { }

                    wire.debug("");
                }
                HttpResponse response;

                try {
                    response = client.execute(post);
                    if( wire.isDebugEnabled() ) {
                        wire.debug(response.getStatusLine().toString());
                        for( Header header : response.getAllHeaders() ) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                    }
                }
                catch( IOException e ) {
                    logger.error("I/O error from server communications: " + e.getMessage());
                    e.printStackTrace();
                    throw new InternalException(e);
                }
                int code = response.getStatusLine().getStatusCode();

                logger.debug("HTTP STATUS: " + code);

                if( code != HttpServletResponse.SC_NO_CONTENT ) {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        try {
                            this.response = EntityUtils.toString(entity);
                        }
                        catch( IOException e ) {
                            throw new CloudException(e);
                        }
                        if( wire.isDebugEnabled() ) {
                            wire.debug(this.response);
                            wire.debug("");
                        }
                    }
                    checkResponse(response, code, this.response);
                }
                else {
                    checkResponse(response, code);
                }
                return code;
            }
            finally {
                if( wire.isDebugEnabled() ) {
                    wire.debug("<<< [POST (" + (new Date()) + ")] -> " + url + "/ <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NimbulaMethod.class.getName() + ".post()");
            }
        }
    }

    @SuppressWarnings("unused")
    public @Nonnegative int put(@Nonnull String targetId, @Nonnull Map<String,Object> state) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + NimbulaMethod.class.getName() + ".put(" + targetId + "," + state + ")");
        }
        try {
            authenticate();
            String target = getUrl(url, targetId);

            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug(">>> [PUT (" + (new Date()) + ")] -> " + target + " >--------------------------------------------------------------------------------------");
            }
            try {
                ProviderContext ctx = cloud.getContext();

                if( ctx == null ) {
                    throw new CloudException("No context was set for this request");
                }
                HttpClient client = getClient(ctx, target.startsWith("https"));
                HttpPut put = new HttpPut(target);

                put.addHeader("Content-Type", "application/json");
                put.setHeader("Cookie", authCookie);
                try {
                    //noinspection deprecation
                    put.setEntity(new StringEntity((new JSONObject(state)).toString(), "application/json", "UTF-8"));
                }
                catch( UnsupportedEncodingException e ) {
                    throw new InternalException(e);
                }

                if( wire.isDebugEnabled() ) {
                    wire.debug(put.getRequestLine().toString());
                    for( Header header : put.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");

                    try { wire.debug(EntityUtils.toString(put.getEntity())); }
                    catch( IOException ignore ) { }

                    wire.debug("");
                }
                HttpResponse response;

                try {
                    response = client.execute(put);
                    if( wire.isDebugEnabled() ) {
                        wire.debug(response.getStatusLine().toString());
                        for( Header header : response.getAllHeaders() ) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                    }
                }
                catch( IOException e ) {
                    logger.error("I/O error from server communications: " + e.getMessage());
                    e.printStackTrace();
                    throw new InternalException(e);
                }
                int code = response.getStatusLine().getStatusCode();

                logger.debug("HTTP STATUS: " + code);

                if( code != HttpServletResponse.SC_NO_CONTENT ) {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        try {
                            this.response = EntityUtils.toString(entity);
                        }
                        catch( IOException e ) {
                            throw new CloudException(e);
                        }
                        if( wire.isDebugEnabled() ) {
                            wire.debug(this.response);
                            wire.debug("");
                        }
                    }
                    checkResponse(response, code, this.response);
                }
                else {
                    checkResponse(response, code);
                }
                return code;
            }
            finally {
                if( wire.isDebugEnabled() ) {
                    wire.debug("<<< [PUT (" + (new Date()) + ")] -> " + url + "/ <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + NimbulaMethod.class.getName() + ".put()");
            }
        }
    }
}