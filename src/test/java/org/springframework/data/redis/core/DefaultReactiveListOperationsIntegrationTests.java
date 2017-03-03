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

import static org.junit.Assume.*;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collection;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ByteBufferObjectFactory;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.RedisConnection;

/**
 * Integration tests for {@link DefaultReactiveListOperations}.
 *
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class DefaultReactiveListOperationsIntegrationTests<K, V> {

	private final ReactiveRedisTemplate<K, V> redisTemplate;
	private final ReactiveListOperations<K, V> listOperations;

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
	public DefaultReactiveListOperationsIntegrationTests(ReactiveRedisTemplate<K, V> redisTemplate,
			ObjectFactory<K> keyFactory, ObjectFactory<V> valueFactory, String label) {

		this.redisTemplate = redisTemplate;
		this.listOperations = redisTemplate.opsForList();
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
	public void shouldLeftPushElement() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.leftPush(key, value1)).expectNext(1L).expectComplete().verify();
		StepVerifier.create(listOperations.leftPush(key, value2)).expectNext(2L).expectComplete().verify();

		StepVerifier.create(listOperations.range(key, 0, -1).flatMap(Flux::fromIterable)).expectNext(value2)
				.expectNext(value1).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldLeftPushElements() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.leftPushAll(key, value1, value2)).expectNext(2L).expectComplete().verify();

		StepVerifier.create(listOperations.range(key, 0, -1).flatMap(Flux::fromIterable)).expectNext(value2)
				.expectNext(value1).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldLeftPopElement() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.leftPushAll(key, value1, value2)).expectNext(2L).expectComplete().verify();

		StepVerifier.create(listOperations.leftPop(key)).expectNext(value2).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldLeftPopElementWithTimeout() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.leftPop(key, Duration.ofSeconds(1))).expectComplete().verify();

		StepVerifier.create(listOperations.leftPushAll(key, value1, value2)).expectNext(2L).expectComplete().verify();

		StepVerifier.create(listOperations.leftPop(key, Duration.ZERO)).expectNext(value2).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldLeftPushElementWithPivot() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		StepVerifier.create(listOperations.leftPushAll(key, value1, value2)).expectNext(2L).expectComplete().verify();

		StepVerifier.create(listOperations.leftPush(key, value1, value3)).expectNext(3L).expectComplete().verify();

		StepVerifier.create(listOperations.range(key, 0, -1).flatMap(Flux::fromIterable)).expectNext(value2)
				.expectNext(value3).expectNext(value1).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldLeftPushIfPresent() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.leftPushIfPresent(key, value1)).expectNext(0L).expectComplete().verify();
		StepVerifier.create(listOperations.leftPush(key, value1)).expectNext(1L).expectComplete().verify();
		StepVerifier.create(listOperations.leftPushIfPresent(key, value2)).expectNext(2L).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldRightPushElement() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPush(key, value1)).expectNext(1L).expectComplete().verify();
		StepVerifier.create(listOperations.rightPush(key, value2)).expectNext(2L).expectComplete().verify();

		StepVerifier.create(listOperations.range(key, 0, -1).flatMap(Flux::fromIterable)).expectNext(value1)
				.expectNext(value2).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldRightPopElement() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPushAll(key, value1, value2)).expectNext(2L).expectComplete().verify();

		StepVerifier.create(listOperations.rightPop(key)).expectNext(value2).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldRightPopElementWithTimeout() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPushAll(key, value1, value2)).expectNext(2L).expectComplete().verify();

		StepVerifier.create(listOperations.rightPop(key, Duration.ZERO)).expectNext(value2).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldRightPushElements() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPushAll(key, value1, value2)).expectNext(2L).expectComplete().verify();

		StepVerifier.create(listOperations.range(key, 0, -1).flatMap(Flux::fromIterable)).expectNext(value1)
				.expectNext(value2).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldRightPushElementWithPivot() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();
		V value3 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPushAll(key, value1, value2)).expectNext(2L).expectComplete().verify();

		StepVerifier.create(listOperations.rightPush(key, value1, value3)).expectNext(3L).expectComplete().verify();

		StepVerifier.create(listOperations.range(key, 0, -1).flatMap(Flux::fromIterable)).expectNext(value1)
				.expectNext(value3).expectNext(value2).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldRightPushIfPresent() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPushIfPresent(key, value1)).expectNext(0L).expectComplete().verify();
		StepVerifier.create(listOperations.rightPush(key, value1)).expectNext(1L).expectComplete().verify();
		StepVerifier.create(listOperations.rightPushIfPresent(key, value2)).expectNext(2L).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldRightPopAndLeftPush() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K source = keyFactory.instance();
		K target = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(listOperations.rightPush(source, value)).expectNext(1L).expectComplete().verify();

		StepVerifier.create(listOperations.rightPopAndLeftPush(source, target)).expectNext(value).expectComplete().verify();

		StepVerifier.create(listOperations.size(source)).expectNext(0L).expectComplete().verify();
		StepVerifier.create(listOperations.size(target)).expectNext(1L).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldRightPopAndLeftPushWithTimeout() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K source = keyFactory.instance();
		K target = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(listOperations.rightPopAndLeftPush(source, target, Duration.ofSeconds(1))).expectComplete()
				.verify();

		StepVerifier.create(listOperations.rightPush(source, value)).expectNext(1L).expectComplete().verify();

		StepVerifier.create(listOperations.rightPopAndLeftPush(source, target, Duration.ZERO)).expectNext(value)
				.expectComplete().verify();

		StepVerifier.create(listOperations.size(source)).expectNext(0L).expectComplete().verify();
		StepVerifier.create(listOperations.size(target)).expectNext(1L).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldReportListSize() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();

		StepVerifier.create(listOperations.size(key)).expectNext(0L).expectComplete().verify();
		StepVerifier.create(listOperations.rightPush(key, value1)).expectNext(1L).expectComplete().verify();
		StepVerifier.create(listOperations.size(key)).expectNext(1L).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldTrim() {

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPushAll(key, value1, value2)).expectNext(2L).expectComplete().verify();

		StepVerifier.create(listOperations.trim(key, 0, 0)).expectNext(true).expectComplete().verify();

		StepVerifier.create(listOperations.size(key)).expectNext(1L).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldSetAtPosition() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPushAll(key, value1, value2)).expectNext(2L).expectComplete().verify();

		StepVerifier.create(listOperations.set(key, 1, value1)).expectNext(true).expectComplete().verify();

		StepVerifier.create(listOperations.range(key, 0, -1).flatMap(Flux::fromIterable)).expectNext(value1)
				.expectNext(value1).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldRemoveAtPosition() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPushAll(key, value1, value2)).expectNext(2L).expectComplete().verify();

		StepVerifier.create(listOperations.remove(key, 1, value1)).expectNext(1L).expectComplete().verify();

		StepVerifier.create(listOperations.range(key, 0, -1).flatMap(Flux::fromIterable)).expectNext(value2)
				.expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void shouldGetAtPosition() {

		assumeFalse(valueFactory instanceof ByteBufferObjectFactory);

		K key = keyFactory.instance();
		V value1 = valueFactory.instance();
		V value2 = valueFactory.instance();

		StepVerifier.create(listOperations.rightPushAll(key, value1, value2)).expectNext(2L).expectComplete().verify();

		StepVerifier.create(listOperations.index(key, 1)).expectNext(value2).expectComplete().verify();
	}
}
