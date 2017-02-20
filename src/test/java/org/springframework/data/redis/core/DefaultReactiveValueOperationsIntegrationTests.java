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

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assume.*;

import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Integration tests for {@link DefaultReactiveValueOperations}.
 *
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class DefaultReactiveValueOperationsIntegrationTests<K, V> {

	private final ReactiveRedisTemplate<K, V> redisTemplate;
	private final ReactiveValueOperations<K, V> valueOperations;

	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<V> valueFactory;

	@Parameters(name = "{3}")
	public static Collection<Object[]> testParams() {
		return ReactiveOperationsTestParams.testParams();
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	/**
	 * @param redisTemplate
	 * @param keyFactory
	 * @param valueFactory
	 * @param label parameterized test label, no further use besides that.
	 */
	public DefaultReactiveValueOperationsIntegrationTests(ReactiveRedisTemplate<K, V> redisTemplate,
			ObjectFactory<K> keyFactory, ObjectFactory<V> valueFactory, String label) {

		this.redisTemplate = redisTemplate;
		this.valueOperations = redisTemplate.opsForValue();
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;

		ConnectionFactoryTracker.add(redisTemplate.getConnectionFactory());
	}

	@Before
	public void before() {

		RedisConnection connection = redisTemplate.getConnectionFactory().getConnection();
		connection.flushAll();
		connection.close();
	}

	@Test // DATAREDIS-602
	public void set() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(valueOperations.set(key, value)).expectNext(true).expectComplete().verify();

		StepVerifier.create(valueOperations.get(key)).expectNext(value).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void setWithExpiry() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(valueOperations.set(key, value, 10, TimeUnit.SECONDS)).expectNext(true).expectComplete()
				.verify();

		StepVerifier.create(valueOperations.get(key)).expectNext(value).expectComplete().verify();

		StepVerifier.create(redisTemplate.getExpire(key)) //
				.consumeNextWith(actual -> assertThat(actual).isGreaterThan(Duration.ofSeconds(8))) //
				.expectComplete() //
				.verify();

	}

	@Test // DATAREDIS-602
	public void setIfAbsent() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(valueOperations.setIfAbsent(key, value)).expectNext(true).expectComplete().verify();

		StepVerifier.create(valueOperations.setIfAbsent(key, value)).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void setIfPresent() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();
		V laterValue = valueFactory.instance();

		StepVerifier.create(valueOperations.setIfPresent(key, value)).expectComplete().verify();

		StepVerifier.create(valueOperations.set(key, value)).expectNext(true).expectComplete().verify();

		StepVerifier.create(valueOperations.setIfPresent(key, laterValue)).expectNext(true).expectComplete().verify();

		StepVerifier.create(valueOperations.get(key)).expectNext(laterValue).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void multiSet() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		Map<K, V> map = new LinkedHashMap<K, V>();
		map.put(key1, value1);
		map.put(key2, value2);

		StepVerifier.create(valueOperations.multiSet(map)).expectNext(true).expectComplete().verify();

		StepVerifier.create(valueOperations.get(key1)).expectNext(value1).expectComplete().verify();
		StepVerifier.create(valueOperations.get(key2)).expectNext(value2).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void multiSetIfAbsent() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		Map<K, V> map = new LinkedHashMap<K, V>();

		map.put(key1, value1);

		StepVerifier.create(valueOperations.multiSetIfAbsent(map)).expectNext(true).expectComplete().verify();

		map.put(key2, value2);
		StepVerifier.create(valueOperations.multiSetIfAbsent(map)).expectNext(false).expectComplete().verify();

		StepVerifier.create(valueOperations.get(key1)).expectNext(value1).expectComplete().verify();
		StepVerifier.create(valueOperations.get(key2)).expectNextCount(0).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void get() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(valueOperations.set(key, value)).expectNext(true).expectComplete().verify();

		StepVerifier.create(valueOperations.get(key)).expectNext(value).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void getAndSet() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();
		V nextValue = valueFactory.instance();

		StepVerifier.create(valueOperations.set(key, value)).expectNext(true).expectComplete().verify();

		StepVerifier.create(valueOperations.getAndSet(key, nextValue)).expectNext(value).expectComplete().verify();

		StepVerifier.create(valueOperations.get(key)).expectNext(nextValue).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void multiGet() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		K absent = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V absentValue = null;

		if (redisTemplate.getValueSerializer() instanceof StringRedisSerializer) {
			absentValue = (V) "";
		}
		if (value1 instanceof ByteBuffer) {
			absentValue = (V) ByteBuffer.wrap(new byte[0]);
		}

		Map<K, V> map = new LinkedHashMap<K, V>();
		map.put(key1, value1);
		map.put(key2, value2);

		StepVerifier.create(valueOperations.multiSet(map)).expectNext(true).expectComplete().verify();

		StepVerifier.create(valueOperations.multiGet(Arrays.asList(key2, key1, absent)))
				.expectNext(Arrays.asList(value2, value1, absentValue)).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void append() {

		assumeTrue(redisTemplate.getValueSerializer() instanceof StringRedisSerializer);

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(valueOperations.set(key, value)).expectNext(true).expectComplete().verify();

		StepVerifier.create(valueOperations.append(key, "foo")).expectNextCount(1).expectComplete().verify();

		StepVerifier.create(valueOperations.get(key)).expectNext((V) (value + "foo")).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void getRange() {

		assumeTrue(redisTemplate.getValueSerializer() instanceof StringRedisSerializer);

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(valueOperations.set(key, value)).expectNext(true).expectComplete().verify();

		String substring = value.toString().substring(1, 5);

		StepVerifier.create(valueOperations.get(key, 1, 4)).expectNext(substring).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void setRange() {

		assumeTrue(redisTemplate.getValueSerializer() instanceof StringRedisSerializer);

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(valueOperations.set(key, value)).expectNext(true).expectComplete().verify();
		StepVerifier.create(valueOperations.set(key, (V) "boo", 2)).expectNextCount(1).expectComplete().verify();

		StepVerifier.create(valueOperations.get(key)).consumeNextWith(actual -> {

			String string = (String) actual;
			String prefix = value.toString().substring(0, 2);

			assertThat(string).startsWith(prefix + "boo");
		}).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void size() {

		assumeTrue(redisTemplate.getValueSerializer() instanceof StringRedisSerializer);

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(valueOperations.set(key, value)).expectNext(true).expectComplete().verify();
		StepVerifier.create(valueOperations.size(key)).expectNext((long) value.toString().length()).expectComplete()
				.verify();
	}

	@Test // DATAREDIS-602
	public void setBit() {

		K key = keyFactory.instance();

		StepVerifier.create(valueOperations.setBit(key, 0, true)).expectNext(false).expectComplete();
		StepVerifier.create(valueOperations.setBit(key, 2, true)).expectNext(false).expectComplete();
	}

	@Test // DATAREDIS-602
	public void getBit() {

		K key = keyFactory.instance();

		StepVerifier.create(valueOperations.setBit(key, 0, true)).expectNext(false).expectComplete();
		StepVerifier.create(valueOperations.getBit(key, 0)).expectNext(true).expectComplete();
		StepVerifier.create(valueOperations.getBit(key, 1)).expectNext(false).expectComplete();
	}
}
