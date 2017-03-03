/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.core;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.serializer.ReactiveSerializationContext;

/**
 * Interface that specified a basic set of Redis operations, implemented by {@link ReactiveRedisTemplate}. Not often
 * used but a useful option for extensibility and testability (as it can be easily mocked or stubbed).
 *
 * @author Mark Paluch
 * @since 2.0
 */
public interface ReactiveRedisOperations<K, V> {

	/**
	 * Executes the given action within a Redis connection. Application exceptions thrown by the action object get
	 * propagated to the caller (can only be unchecked) whenever possible. Redis exceptions are transformed into
	 * appropriate DAO ones. Allows for returning a result object, that is a domain object or a collection of domain
	 * objects. Performs automatic serialization/deserialization for the given objects to and from binary data suitable
	 * for the Redis storage. Note: Callback code is not supposed to handle transactions itself! Use an appropriate
	 * transaction manager. Generally, callback code must not touch any Connection lifecycle methods, like close, to let
	 * the template do its work.
	 *
	 * @param <T> return type
	 * @param action callback object that specifies the Redis action
	 * @return a result object returned by the action or <tt>null</tt>
	 */
	<T> Flux<T> execute(ReactiveRedisCallback<T> action);

	// -------------------------------------------------------------------------
	// Methods dealing with Redis Keys
	// -------------------------------------------------------------------------

	/**
	 * Determine if given {@code key} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/exists">Redis Documentation: EXISTS</a>
	 */
	Mono<Boolean> hasKey(K key);

	/**
	 * Determine the type stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/type">Redis Documentation: TYPE</a>
	 */
	Mono<DataType> type(K key);

	/**
	 * Find all keys matching the given {@code pattern}.
	 *
	 * @param pattern must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/keys">Redis Documentation: KEYS</a>
	 */
	Flux<K> keys(K pattern);

	/**
	 * Return a random key from the keyspace.
	 *
	 * @return
	 * @see <a href="http://redis.io/commands/randomkey">Redis Documentation: RANDOMKEY</a>
	 */
	Mono<K> randomKey();

	/**
	 * Rename key {@code oldKey} to {@code newKey}.
	 *
	 * @param oldKey must not be {@literal null}.
	 * @param newKey must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/rename">Redis Documentation: RENAME</a>
	 */
	Mono<Boolean> rename(K oldKey, K newKey);

	/**
	 * Rename key {@code oleName} to {@code newKey} only if {@code newKey} does not exist.
	 *
	 * @param oldKey must not be {@literal null}.
	 * @param newKey must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/renamenx">Redis Documentation: RENAMENX</a>
	 */
	Mono<Boolean> renameIfAbsent(K oldKey, K newKey);

	/**
	 * Delete given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return The number of keys that were removed.
	 * @see <a href="http://redis.io/commands/del">Redis Documentation: DEL</a>
	 */
	Mono<Long> delete(K... key);

	/**
	 * Delete given {@code keys}.
	 *
	 * @param keys must not be {@literal null}.
	 * @return The number of keys that were removed.
	 * @see <a href="http://redis.io/commands/del">Redis Documentation: DEL</a>
	 */
	Mono<Long> delete(Publisher<K> keys);

	/**
	 * Set time to live for given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout must not be {@literal null}.
	 * @return
	 */
	Mono<Boolean> expire(K key, Duration timeout);

	/**
	 * Set the expiration for given {@code key} as a {@literal expireAt} timestamp.
	 *
	 * @param key must not be {@literal null}.
	 * @param expireAt must not be {@literal null}.
	 * @return
	 */
	Mono<Boolean> expireAt(K key, Instant expireAt);

	/**
	 * Remove the expiration from given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @see <a href="http://redis.io/commands/persist">Redis Documentation: PERSIST</a>
	 */
	Mono<Boolean> persist(K key);

	/**
	 * Move given {@code key} to database with {@code index}.
	 *
	 * @param key must not be {@literal null}.
	 * @param dbIndex
	 * @return
	 * @see <a href="http://redis.io/commands/move">Redis Documentation: MOVE</a>
	 */
	Mono<Boolean> move(K key, int dbIndex);

	/**
	 * Get the time to live for {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return the {@link Duration} of the associated key. {@link Duration#ZERO} if no timeout associated or empty
	 *         {@link Mono} if the key does not exist.
	 * @see <a href="http://redis.io/commands/pttl">Redis Documentation: PTTL</a>
	 */
	Mono<Duration> getExpire(K key);

	// -------------------------------------------------------------------------
	// Methods to obtain specific operations interface objects.
	// -------------------------------------------------------------------------

	// operation types
	/**
	 * Returns the operations performed on simple values (or Strings in Redis terminology).
	 *
	 * @return value operations
	 */
	ReactiveValueOperations<K, V> opsForValue();

	/**
	 * Returns the operations performed on list values.
	 *
	 * @return list operations
	 */
	ReactiveListOperations<K, V> opsForList();

	/**
	 * Returns the operations performed on multisets using HyperLogLog.
	 *
	 * @return never {@literal null}.
	 */
	ReactiveHyperLogLogOperations<K, V> opsForHyperLogLog();

	/**
	 * Returns the operations performed on hash values.
	 *
	 * @param <HK> hash key (or field) type
	 * @param <HV> hash value type
	 * @return hash operations
	 */
	<HK, HV> ReactiveHashOperations<K, HK, HV> opsForHash();

	/**
	 * Returns geospatial specific operations interface.
	 *
	 * @return never {@literal null}.
	 */
	ReactiveGeoOperations<K, V> opsForGeo();

	/**
	 * @return the {@link ReactiveSerializationContext}.
	 */
	ReactiveSerializationContext<K, V> serialization();
}
