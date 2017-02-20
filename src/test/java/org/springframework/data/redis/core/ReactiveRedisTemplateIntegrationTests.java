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

import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnection;

/**
 * Integration tests for {@link ReactiveRedisTemplate}.
 *
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class ReactiveRedisTemplateIntegrationTests<K, V> {

	private final ReactiveRedisTemplate<K, V> redisTemplate;

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
	public ReactiveRedisTemplateIntegrationTests(ReactiveRedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<V> valueFactory, String label) {

		this.redisTemplate = redisTemplate;
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
	public void exists() {

		K key = keyFactory.instance();

		StepVerifier.create(redisTemplate.hasKey(key)).expectNext(false).expectComplete().verify();

		redisTemplate.opsForValue().set(key, valueFactory.instance()).block();

		StepVerifier.create(redisTemplate.hasKey(key)).expectNext(true).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void type() {

		K key = keyFactory.instance();

		StepVerifier.create(redisTemplate.type(key)).expectNext(DataType.NONE).expectComplete().verify();

		redisTemplate.opsForValue().set(key, valueFactory.instance()).block();

		StepVerifier.create(redisTemplate.type(key)).expectNext(DataType.STRING).expectComplete().verify();
	}

	@Test // DATAREDIS-602
	public void rename() {

		K oldName = keyFactory.instance();
		K newName = keyFactory.instance();

		redisTemplate.opsForValue().set(oldName, valueFactory.instance()).block();

		StepVerifier.create(redisTemplate.rename(oldName, newName)) //
				.expectNext(true) //
				.expectComplete() //
				.verify();
	}

	@Test // DATAREDIS-602
	public void renameNx() {

		K oldName = keyFactory.instance();
		K existing = keyFactory.instance();
		K newName = keyFactory.instance();

		redisTemplate.opsForValue().set(oldName, valueFactory.instance()).block();
		redisTemplate.opsForValue().set(existing, valueFactory.instance()).block();

		StepVerifier.create(redisTemplate.renameIfAbsent(oldName, newName)) //
				.expectNext(true) //
				.expectComplete() //
				.verify();

		redisTemplate.opsForValue().set(existing, valueFactory.instance()).block();

		StepVerifier.create(redisTemplate.renameIfAbsent(newName, existing)).expectNext(false) //
				.expectComplete() //
				.verify();
	}

	@Test // DATAREDIS-602
	public void expire() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		redisTemplate.opsForValue().set(key, value).block();

		StepVerifier.create(redisTemplate.expire(key, Duration.ofSeconds(10))) //
				.expectNext(true) //
				.expectComplete() //
				.verify();

		StepVerifier.create(redisTemplate.getExpire(key)) //
				.consumeNextWith(actual -> assertThat(actual).isGreaterThan(Duration.ofSeconds(8)))//
				.expectComplete() //
				.verify();
	}

	@Test // DATAREDIS-602
	public void preciseExpire() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		redisTemplate.opsForValue().set(key, value).block();

		StepVerifier.create(redisTemplate.expire(key, Duration.ofMillis(10_001))) //
				.expectNext(true) //
				.expectComplete() //
				.verify();

		StepVerifier.create(redisTemplate.getExpire(key)) //
				.consumeNextWith(actual -> assertThat(actual).isGreaterThan(Duration.ofSeconds(8))) //
				.expectComplete() //
				.verify();
	}

	@Test // DATAREDIS-602
	public void expireAt() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		redisTemplate.opsForValue().set(key, value).block();

		Instant expireAt = Instant.ofEpochSecond(Instant.now().plus(Duration.ofSeconds(10)).getEpochSecond());

		StepVerifier.create(redisTemplate.expireAt(key, expireAt)) //
				.expectNext(true) //
				.expectComplete() //
				.verify();

		StepVerifier.create(redisTemplate.getExpire(key)) //
				.consumeNextWith(actual -> assertThat(actual).isGreaterThan(Duration.ofSeconds(8))) //
				.expectComplete() //
				.verify();
	}

	@Test // DATAREDIS-602
	public void preciseExpireAt() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		redisTemplate.opsForValue().set(key, value).block();

		Instant expireAt = Instant.ofEpochSecond(Instant.now().plus(Duration.ofSeconds(10)).getEpochSecond(), 5);

		StepVerifier.create(redisTemplate.expireAt(key, expireAt)) //
				.expectNext(true) //
				.expectComplete() //
				.verify();

		StepVerifier.create(redisTemplate.getExpire(key)) //
				.consumeNextWith(actual -> assertThat(actual).isGreaterThan(Duration.ofSeconds(8))) //
				.expectComplete() //
				.verify();
	}

	@Test // DATAREDIS-602
	public void getTtlForAbsentKeyShouldCompleteWithoutValue() {

		K key = keyFactory.instance();

		StepVerifier.create(redisTemplate.getExpire(key)) //
				.expectComplete() //
				.verify();
	}

	@Test // DATAREDIS-602
	public void getTtlForKeyWithoutExpiryShouldCompleteWithZeroDuration() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		redisTemplate.opsForValue().set(key, value).block();

		StepVerifier.create(redisTemplate.getExpire(key)) //
				.expectNext(Duration.ZERO).expectComplete() //
				.verify();
	}

	@Test // DATAREDIS-602
	public void move() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		redisTemplate.opsForValue().set(key, value).block();

		StepVerifier.create(redisTemplate.move(key, 5)) //
				.expectNext(true) //
				.expectComplete() //
				.verify();

		StepVerifier.create(redisTemplate.hasKey(key)) //
				.expectNext(false) //
				.expectComplete() //
				.verify();
	}
}
