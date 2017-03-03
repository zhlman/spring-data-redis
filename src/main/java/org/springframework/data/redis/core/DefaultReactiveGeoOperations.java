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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.reactivestreams.Publisher;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.ReactiveGeoCommands;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs;
import org.springframework.data.redis.serializer.ReactiveSerializationContext;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ReactiveGeoOperations}.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class DefaultReactiveGeoOperations<K, V> implements ReactiveGeoOperations<K, V> {

	private final ReactiveRedisTemplate<K, V> template;

	public DefaultReactiveGeoOperations(ReactiveRedisTemplate<K, V> template) {
		this.template = template;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveGeoOperations#geoAdd(java.lang.Object, org.springframework.data.geo.Point, java.lang.Object)
	 */
	@Override
	public Mono<Long> geoAdd(K key, Point point, V member) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(point, "Point must not be null!");
		Assert.notNull(member, "Member must not be null!");

		return createMono(connection -> connection.geoAdd(rawKey(key), point, rawValue(member)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveGeoOperations#geoAdd(java.lang.Object, org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation)
	 */
	@Override
	public Mono<Long> geoAdd(K key, GeoLocation<V> location) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(location, "GeoLocation must not be null!");

		return createMono(connection -> connection.geoAdd(rawKey(key),
				new GeoLocation<>(rawValue(location.getName()), location.getPoint())));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveGeoOperations#geoAdd(java.lang.Object, java.util.Map)
	 */
	@Override
	public Mono<Long> geoAdd(K key, Map<V, Point> memberCoordinateMap) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(memberCoordinateMap, "Map must not be null!");

		return createMono(connection -> {

			Mono<List<GeoLocation<ByteBuffer>>> serializedList = Flux
					.fromIterable(() -> memberCoordinateMap.entrySet().iterator())
					.map(entry -> new GeoLocation<>(rawValue(entry.getKey()), entry.getValue())).collectList();

			return serializedList.flatMap(list -> connection.geoAdd(rawKey(key), list));
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveGeoOperations#geoAdd(java.lang.Object, java.lang.Iterable)
	 */
	@Override
	public Mono<Long> geoAdd(K key, Iterable<GeoLocation<V>> geoLocations) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(geoLocations, "GeoLocations must not be null!");

		return createMono(connection -> {

			Mono<List<GeoLocation<ByteBuffer>>> serializedList = Flux.fromIterable(geoLocations)
					.map(location -> new GeoLocation<>(rawValue(location.getName()), location.getPoint())).collectList();

			return serializedList.flatMap(list -> connection.geoAdd(rawKey(key), list));
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveGeoOperations#geoAdd(java.lang.Object, org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<Long> geoAdd(K key, Publisher<? extends Collection<GeoLocation<V>>> locations) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(locations, "GeoLocations must not be null!");

		return createFlux(connection -> {

			return Flux.from(locations)
					.map(locationList -> locationList.stream()
							.map(location -> new GeoLocation<>(rawValue(location.getName()), location.getPoint()))
							.collect(Collectors.toList()))
					.flatMap(list -> connection.geoAdd(rawKey(key), list));
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveGeoOperations#geoDist(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Distance> geoDist(K key, V member1, V member2) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(member1, "Member 1 must not be null!");
		Assert.notNull(member2, "Member 2 must not be null!");

		return createMono(connection -> connection.geoDist(rawKey(key), rawValue(member1), rawValue(member2)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveGeoOperations#geoDist(java.lang.Object, java.lang.Object, java.lang.Object, org.springframework.data.geo.Metric)
	 */
	@Override
	public Mono<Distance> geoDist(K key, V member1, V member2, Metric metric) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(member1, "Member 1 must not be null!");
		Assert.notNull(member2, "Member 2 must not be null!");
		Assert.notNull(metric, "Metric must not be null!");

		return createMono(connection -> connection.geoDist(rawKey(key), rawValue(member1), rawValue(member2), metric));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveGeoOperations#geoHash(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<String> geoHash(K key, V member) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(member, "Member must not be null!");

		return createMono(connection -> connection.geoHash(rawKey(key), rawValue(member)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveGeoOperations#geoHash(java.lang.Object, java.lang.Object[])
	 */
	@Override
	@SafeVarargs
	public final Mono<List<String>> geoHash(K key, V... members) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(members, "Member must not be null!");
		Assert.notEmpty(members, "Members must not be empty!");
		Assert.noNullElements(members, "Members must not contain null elements!");

		return createMono(connection -> {

			return Flux.fromArray(members) //
					.map(this::rawValue) //
					.collectList() //
					.flatMap(serialized -> connection.geoHash(rawKey(key), serialized));
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveGeoOperations#geoPos(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Mono<Point> geoPos(K key, V member) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(member, "Member must not be null!");

		return createMono(connection -> connection.geoPos(rawKey(key), rawValue(member)));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveGeoOperations#geoPos(java.lang.Object, java.lang.Object[])
	 */
	@Override
	@SafeVarargs
	public final Mono<List<Point>> geoPos(K key, V... members) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(members, "Member must not be null!");
		Assert.notEmpty(members, "Members must not be empty!");
		Assert.noNullElements(members, "Members must not contain null elements!");

		return createMono(connection -> {

			return Flux.fromArray(members) //
					.map(this::rawValue) //
					.collectList() //
					.flatMap(serialized -> connection.geoPos(rawKey(key), serialized));
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveGeoOperations#geoRadius(java.lang.Object, org.springframework.data.geo.Circle)
	 */
	@Override
	public Mono<List<GeoLocation<V>>> geoRadius(K key, Circle within) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(within, "Circle must not be null!");

		return createMono(connection -> {

			return connection.geoRadius(rawKey(key), within) //
					.flatMap(Flux::fromIterable) //
					.map(location -> new GeoLocation<>(readValue(location.getName()), location.getPoint())) //
					.collectList();
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveGeoOperations#geoRadius(java.lang.Object, org.springframework.data.geo.Circle, org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs)
	 */
	@Override
	public Mono<GeoResults<GeoLocation<V>>> geoRadius(K key, Circle within, GeoRadiusCommandArgs args) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(within, "Circle must not be null!");
		Assert.notNull(args, "GeoRadiusCommandArgs must not be null!");

		return createMono(connection -> {

			return connection.geoRadius(rawKey(key), within, args) //
					.flatMap(Flux::fromIterable) //
					.map(geoResult -> new GeoResult<>(
							new GeoLocation<>(readValue(geoResult.getContent().getName()), geoResult.getContent().getPoint()),
							geoResult.getDistance())) //
					.collectList() //
					.map(GeoResults::new);
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveGeoOperations#geoRadiusByMember(java.lang.Object, java.lang.Object, double)
	 */
	@Override
	public Mono<List<GeoLocation<V>>> geoRadiusByMember(K key, V member, double radius) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(member, "Member must not be null!");

		return createMono(connection -> {

			return connection.geoRadiusByMember(rawKey(key), rawValue(member), new Distance(radius)) //
					.flatMap(Flux::fromIterable) //
					.map(geoLocation -> new GeoLocation<>(readValue(geoLocation.getName()), geoLocation.getPoint())) //
					.collectList();
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveGeoOperations#geoRadiusByMember(java.lang.Object, java.lang.Object, org.springframework.data.geo.Distance)
	 */
	@Override
	public Mono<List<GeoLocation<V>>> geoRadiusByMember(K key, V member, Distance distance) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(member, "Member must not be null!");
		Assert.notNull(distance, "Distance must not be null!");

		return createMono(connection -> {

			return connection.geoRadiusByMember(rawKey(key), rawValue(member), distance) //
					.flatMap(Flux::fromIterable) //
					.map(geoLocation -> new GeoLocation<>(readValue(geoLocation.getName()), geoLocation.getPoint())) //
					.collectList();
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveGeoOperations#geoRadiusByMember(java.lang.Object, java.lang.Object, org.springframework.data.geo.Distance, org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs)
	 */
	@Override
	public Mono<GeoResults<GeoLocation<V>>> geoRadiusByMember(K key, V member, Distance distance,
			GeoRadiusCommandArgs args) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(member, "Member must not be null!");
		Assert.notNull(distance, "Distance must not be null!");
		Assert.notNull(args, "GeoRadiusCommandArgs must not be null!");

		return createMono(connection -> {

			return connection.geoRadiusByMember(rawKey(key), rawValue(member), distance, args) //
					.flatMap(Flux::fromIterable) //
					.map(geoResult -> new GeoResult<>(
							new GeoLocation<>(readValue(geoResult.getContent().getName()), geoResult.getContent().getPoint()),
							geoResult.getDistance())) //
					.collectList() //
					.map(GeoResults::new);
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveGeoOperations#geoRemove(java.lang.Object, java.lang.Object[])
	 */
	@Override
	@SafeVarargs
	public final Mono<Long> geoRemove(K key, V... members) {

		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(members, "Member must not be null!");
		Assert.notEmpty(members, "Members must not be empty!");
		Assert.noNullElements(members, "Members must not contain null elements!");

		return template.createMono(connection -> {

			return Flux.fromArray(members) //
					.map(this::rawValue) //
					.collectList() //
					.flatMap(serialized -> connection.zSetCommands().zRem(rawKey(key), serialized));
		});

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.ReactiveGeoOperations#getOperations()
	 */
	@Override
	public ReactiveRedisOperations<K, V> getOperations() {
		return template;
	}

	private <T> Mono<T> createMono(Function<ReactiveGeoCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null!");

		return template.createMono(connection -> function.apply(connection.geoCommands()));
	}

	private <T> Flux<T> createFlux(Function<ReactiveGeoCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null!");

		return template.createFlux(connection -> function.apply(connection.geoCommands()));
	}

	private ByteBuffer rawKey(K key) {
		return serialization().key().writer().write(key);
	}

	private ByteBuffer rawValue(V value) {
		return serialization().value().writer().write(value);
	}

	private V readValue(ByteBuffer buffer) {
		return serialization().value().reader().read(buffer);
	}

	private ReactiveSerializationContext<K, V> serialization() {
		return template.serialization();
	}
}
