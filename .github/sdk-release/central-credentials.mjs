import { open } from 'node:fs/promises';
import { constants } from 'node:fs';
import { isAbsolute } from 'node:path';
import { githubPublisherIdentity } from './github-publisher-identity.mjs';

/** File must be provisioned separately by the protected release environment.
 * Central's long-lived token has no native GitHub/repository scope; enforce
 * workload identity here before reading it. This does not replace candidate,
 * compatibility or current-publication-intent checks in centralPublisher. */
export function centralFileCredentials({ path, configuration: inputConfiguration,
  environment: inputEnvironment, maskCredential, tokenProvider, fetchImpl = fetch, now = Date.now }) {
  const configuration = structuredClone(inputConfiguration), environment = { ...inputEnvironment };
  if (!isAbsolute(path ?? '') || !['java', 'kotlin'].includes(configuration?.family) || typeof maskCredential !== 'function')
    throw new Error('Explicit Central credential file, JVM publisher and masking function required');
  return async request => {
    const family = configuration.family, repository = `reacon-io/reacon-${family}`;
    if (request.registry !== 'maven-central' || request.family !== family || request.repository !== repository ||
        request.namespace !== 'io.reacon' || request.workflow !== 'publish.yml' || request.environment !== 'release')
      throw new Error('Central credential request differs from the configured JVM publisher');
    // Signature verification is performed afresh for each credential acquisition;
    // an earlier JSON identity report is not accepted as authorization.
    const identity = await githubPublisherIdentity({ configuration, environment, tokenProvider, fetchImpl, now });
    let bytes, credential;
    try {
      const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
      try {
        const stat = await file.stat();
        if (!stat.isFile() || stat.uid !== process.getuid() || (stat.mode & 0o077) !== 0 || stat.size < 1 || stat.size > 8192)
          throw new Error();
        bytes = await file.readFile();
        if (bytes.length !== stat.size) throw new Error();
        credential = JSON.parse(bytes);
      } finally { await file.close(); }
      if (credential.formatVersion !== 1 || credential.kind !== 'central-user-token' || credential.registry !== 'maven-central' ||
          credential.accountEmail !== 'achazal@reacon.io' || credential.namespace !== 'io.reacon' ||
          !Array.isArray(credential.allowedFamilies) || !credential.allowedFamilies.includes(family) ||
          credential.allowedFamilies.some(value => !['java','kotlin'].includes(value)) ||
          typeof credential.username !== 'string' || !/^[A-Za-z0-9_-]{1,128}$/.test(credential.username) ||
          typeof credential.password !== 'string' || !/^\S{20,512}$/.test(credential.password) ||
          !Number.isFinite(credential.expiresAt) || credential.expiresAt <= now() + 30000)
        throw new Error();
      for (const value of [credential.username, credential.password, Buffer.from(`${credential.username}:${credential.password}`).toString('base64')])
        maskCredential(value);
    } catch { throw new Error('Central credentials are unavailable, expired or do not match company policy'); }
    finally { bytes?.fill(0); }
    const expiresAt = Math.min(credential.expiresAt, Date.parse(identity.tokenExpiresAt));
    if (!Number.isFinite(expiresAt) || expiresAt <= now() + 30000) throw new Error('Central credential identity expired');
    return { kind: 'central-user-token', registry: 'maven-central', namespace: 'io.reacon',
      accountEmail: 'achazal@reacon.io', repository, workflow: 'publish.yml', environment: 'release',
      username: credential.username, password: credential.password, expiresAt,
      workerId: identity.workerId, identityClaimsSha256: identity.verifiedClaimsSha256 };
  };
}
