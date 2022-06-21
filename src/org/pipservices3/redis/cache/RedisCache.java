package org.pipservices3.redis.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.pipservices3.commons.config.ConfigParams;
import org.pipservices3.commons.config.IConfigurable;
import org.pipservices3.commons.convert.JsonConverter;
import org.pipservices3.commons.errors.ApplicationException;
import org.pipservices3.commons.errors.ConfigException;
import org.pipservices3.commons.errors.ConnectionException;
import org.pipservices3.commons.errors.InvalidStateException;
import org.pipservices3.commons.refer.IReferenceable;
import org.pipservices3.commons.refer.IReferences;
import org.pipservices3.commons.run.IOpenable;
import org.pipservices3.components.auth.CredentialResolver;
import org.pipservices3.components.cache.ICache;
import org.pipservices3.components.connect.ConnectionResolver;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.params.SetParams;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;


/**
 * Distributed cache that stores values in Redis in-memory database.
 * <p>
 * ### Configuration parameters ###
 *
 * <pre>
 * - connection(s):
 *   - discovery_key:         (optional) a key to retrieve the connection from {@link org.pipservices3.components.connect.IDiscovery}
 *   - host:                  host name or IP address
 *   - port:                  port number
 *   - uri:                   resource URI or connection string with all parameters in it
 * - credential(s):
 *   - store_key:             key to retrieve parameters from credential store
 *   - username:              user name (currently is not used)
 *   - password:              user password
 * - options:
 *   - retries:               number of retries (default: 3)
 *   - timeout:               default caching timeout in milliseconds (default: 1 minute)
 *   - max_size:              maximum number of values stored in this cache (default: 1000)
 * </pre>
 * <p>
 * ### References ###
 * <p>
 * - *:discovery:*:*:1.0        (optional) {@link org.pipservices3.components.connect.IDiscovery} services to resolve connection
 * - *:credential-store:*:*:1.0 (optional) Credential stores to resolve credential
 * <p>
 * ### Example ###
 * <pre>
 * {@code
 * var cache = new RedisCache();
 * cache.configure(ConfigParams.fromTuples(
 *         "connection.host", "localhost",
 *         "connection.port", 6379
 * ));
 * cache.open("123");
 * cache.store("123", "key1", "ABC", 5000);
 * var value = cache.retrieve("123", "key1"); // Result: "ABC"
 * }
 * </pre>
 */
public class RedisCache implements ICache, IConfigurable, IReferenceable, IOpenable {

    private final ConnectionResolver _connectionResolver = new ConnectionResolver();
    private final CredentialResolver _credentialResolver = new CredentialResolver();

    private int _timeout = 30000;
    private int _retries = 3;

    private Jedis _client;

    /**
     * Configures component by passing configuration parameters.
     *
     * @param config configuration parameters to be set.
     */
    @Override
    public void configure(ConfigParams config) {
        this._connectionResolver.configure(config);
        this._credentialResolver.configure(config);

        this._timeout = config.getAsIntegerWithDefault("options.timeout", this._timeout);
        this._retries = config.getAsIntegerWithDefault("options.retries", this._retries);
    }

    /**
     * Sets references to dependent components.
     *
     * @param references references to locate the component dependencies.
     */
    @Override
    public void setReferences(IReferences references) {
        this._connectionResolver.setReferences(references);
        this._credentialResolver.setReferences(references);
    }

    /**
     * Checks if the component is opened.
     *
     * @return true if the component has been opened and false otherwise.
     */
    @Override
    public boolean isOpen() {
        return _client != null;
    }

    /**
     * Opens the component.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     */
    @Override
    public void open(String correlationId) throws ApplicationException {
        var connection = this._connectionResolver.resolve(correlationId);
        var credential = this._credentialResolver.lookup(correlationId);

        if (connection == null)
            throw new ConfigException(
                    correlationId,
                    "NO_CONNECTION",
                    "Connection is not configured"
            );

        // Retry strategy
        Jedis jedis = null;
        var startTime = ZonedDateTime.now();
        var tryCount = 1;
        for (; tryCount <= _retries; tryCount++) {
            try {
                if ((ZonedDateTime.now().toInstant().toEpochMilli() - startTime.toInstant().toEpochMilli()) >= _timeout)
                    throw new RuntimeException(
                            new ConnectionException(
                                    correlationId,
                                    "NO_CONNECTION",
                                    "Redis Connection timeout"
                            )
                    );

                if (jedis != null && jedis.isConnected())
                    break;

                jedis = new Jedis(
                        connection.getAsStringWithDefault("host", "localhost"),
                        connection.getAsIntegerWithDefault("port", 6379)
                );

                jedis.connect();
            } catch (JedisConnectionException ex) {
                if (tryCount >= _retries)
                    throw new RuntimeException(ex);
            }
        }

        if (credential != null && credential.getPassword() != null)
            jedis.auth(credential.getPassword());

        _client = jedis;
    }

    /**
     * Closes component and frees used resources.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     */
    @Override
    public void close(String correlationId) {
        if (this._client == null) return;

        this._client.close();
        this._client.quit();
        this._client = null;
    }

    private void checkOpened(String correlationId) {
        if (!this.isOpen()) {
            throw new RuntimeException(
                    new InvalidStateException(
                            correlationId,
                            "NOT_OPENED",
                            "Connection is not opened"
                    )
            );
        }
    }

    /**
     * Retrieves cached value from the cache using its key.
     * If value is missing in the cache or expired it returns null.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param key           a unique value key.
     * @return a retrieve cached value or <code>null</code> if nothing was found.
     */
    @Override
    public Object retrieve(String correlationId, String key) {
        this.checkOpened(correlationId);

        return _client.get(key);
    }

    /**
     * Stores value in the cache with expiration time.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param key           a unique value key.
     * @param value         a value to store.
     * @param timeout       expiration timeout in milliseconds.
     * @return the stored value.
     */
    @Override
    public Object store(String correlationId, String key, Object value, long timeout) {
        try {
            this.checkOpened(correlationId);
            String cacheValue;

            if (value instanceof String || value == null)
                cacheValue = String.valueOf(value);
            else if (value instanceof ZonedDateTime)
                cacheValue = ((ZonedDateTime) value).withZoneSameInstant(ZoneId.of("UTC"))
                        .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            else
                cacheValue = JsonConverter.toJson(value);

            return this._client.set(key, cacheValue, new SetParams().px(timeout));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Removes a value from the cache by its key.
     *
     * @param correlationId (optional) transaction id to trace execution through call chain.
     * @param key           a unique value key.
     */
    @Override
    public void remove(String correlationId, String key) {
        this.checkOpened(correlationId);
        _client.getDel(key);
    }
}
