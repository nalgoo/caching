<?php

namespace Nalgoo\Caching;

use Aws\S3\Exception\S3Exception;
use Aws\S3\S3Client;
use Psr\Clock\ClockInterface;
use Psr\SimpleCache\CacheInterface;

class S3StringSimpleCache implements CacheInterface
{
	public function __construct(
		private readonly S3Client       $client,
		private readonly string         $bucket,
		private readonly ClockInterface $clock,
	) {
	}

	public function get(string $key, mixed $default = null): string
	{
		$this->assertKeyIsValid($key);

		try {
			$result = $this->client->getObject([
				'Bucket' => $this->bucket,
				'Key' => $key,
			]);
		} catch (S3Exception $e) {
			if ($e->getAwsErrorCode() === 'NoSuchKey') {
				return $default;
			}
			throw $e;
		}

		if ($result['Metadata']['expires_at'] ?? null) {
			$expiresAt = (int) $result['Metadata']['expires_at'];
			if ($this->clock->now()->getTimestamp() >= $expiresAt) {
				return $default;
			}
		}

		return (string) $result['Body'];
	}

	public function set(string $key, mixed $value, \DateInterval|int $ttl = null): bool
	{
		$this->assertKeyIsValid($key);

		if (!is_string($value)) {
			return false;
		}

		try {
			$meta = [];

			if ($ttl !== null) {
				$meta['expires_at'] = $this->clock
					->now()
					->add($ttl instanceof \DateInterval
						? $ttl
						: new \DateInterval('PT' . $ttl . 'S'))
					->getTimestamp();
			}

			$this->client->putObject([
				'Bucket' => $this->bucket,
				'Key' => $key,
				'Body' => $value,
				'ACL' => 'private',
				'ContentType' => 'application/json',
				'Metadata' => $meta,
			]);

			return true;
		} catch (S3Exception $e) {
			return false;
		}
	}

	public function delete(string $key): bool
	{
		$this->assertKeyIsValid($key);

		try {
			$this->client->deleteObject([
				'Bucket' => $this->bucket,
				'Key' => $key,
			]);

			return true;
		} catch (S3Exception $e) {
			if ($e->getAwsErrorCode() === 'NoSuchKey') {
				return false;
			}
			throw $e;
		}
	}

	public function clear(): bool
	{
		throw new \BadMethodCallException('Not implemented');
	}

	public function getMultiple(iterable $keys, mixed $default = null): iterable
	{
		return array_map(fn($key) => $this->get($key, $default), is_array($keys) ? $keys : iterator_to_array($keys));
	}

	public function setMultiple(iterable $values, \DateInterval|int $ttl = null): bool
	{
		foreach ($values as $key => $value) {
			if (!$this->set($key, $value, $ttl)) {
				return false;
			}
		}

		return true;
	}

	public function deleteMultiple(iterable $keys): bool
	{
		foreach ($keys as $key) {
			if (!$this->delete($key)) {
				return false;
			}
		}

		return true;
	}

	public function has(string $key): bool
	{
		$this->assertKeyIsValid($key);

		return $this->client->doesObjectExistV2($this->bucket, $key);
	}

	private function assertKeyIsValid(string $key): void
	{
		if (!preg_match('/^[a-zA-Z0-9\-_.]{1,64}$/', $key)) {
			throw new \InvalidArgumentException('Invalid key');
		}
	}
}
