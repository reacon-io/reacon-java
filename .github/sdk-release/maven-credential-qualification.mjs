import { createHash, randomUUID } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, readFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { parseMavenSignatureStatus } from './maven-signature-status.mjs';

const execute = promisify(execFile);
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');

/** Read-only Central authentication probe. A random absent deployment cannot
 * establish namespace upload permission or create a deployment/version. */
export async function qualifyCentralAuthentication({ credentials, fetchImpl = fetch, now = Date.now }) {
  if (credentials?.kind !== 'central-user-token' || credentials.registry !== 'maven-central' ||
      credentials.accountEmail !== 'achazal@reacon.io' || credentials.namespace !== 'io.reacon' ||
      !['reacon-io/reacon-java','reacon-io/reacon-kotlin'].includes(credentials.repository) ||
      credentials.workflow !== 'publish.yml' || credentials.environment !== 'release' ||
      typeof credentials.username !== 'string' || !/^[A-Za-z0-9_-]{1,128}$/.test(credentials.username) ||
      typeof credentials.password !== 'string' || !/^\S{20,512}$/.test(credentials.password) ||
      !Number.isFinite(credentials.expiresAt) || credentials.expiresAt <= now()+30000)
    throw new Error('Valid company Central credentials required for qualification');
  const endpoint = `https://central.sonatype.com/api/v1/publisher/status?id=${randomUUID()}`;
  const statuses = [];
  for (const authenticated of [false,true]) {
    let response;
    try {
      response = await fetchImpl(endpoint, { method:'POST', redirect:'error', signal:AbortSignal.timeout(20000),
        headers:authenticated ? { Authorization:'Bearer '+Buffer.from(`${credentials.username}:${credentials.password}`).toString('base64') } : {} });
      statuses.push(response.status);
      await response.body?.cancel();
    } catch { throw new Error('Central credential qualification transport failed; details suppressed'); }
  }
  if (statuses[0]!==401 || statuses[1]!==404) throw new Error('Central authentication probe did not distinguish rejected and accepted credentials');
  return { registry:'maven-central', namespace:'io.reacon', repository:credentials.repository,
    anonymousStatus:statuses[0], authenticatedStatus:statuses[1], authenticationAccepted:true,
    namespaceUploadVerified:false, packageUploaded:false };
}

/** Prove that the configured encrypted primary key can sign and verify a fresh
 * synthetic challenge. No SDK/package file is accepted by this function. */
export async function qualifyMavenSigningKey({ privateKey, passphrase, publicKey, policy, challenge }) {
  const privateBytes=Buffer.from(privateKey), publicBytes=Buffer.from(publicKey);
  if (!privateBytes.toString('ascii').startsWith('-----BEGIN PGP PRIVATE KEY BLOCK-----') || privateBytes.length>65536 ||
      !publicBytes.toString('ascii').startsWith('-----BEGIN PGP PUBLIC KEY BLOCK-----') || publicBytes.length>65536 ||
      publicBytes.includes(Buffer.from('PRIVATE KEY')) || typeof passphrase!=='string' || !/^[^\r\n]{32,1024}$/.test(passphrase) ||
      policy?.kind!=='sdk-maven-signing-key' || policy.signingMode!=='primary-key-only' ||
      !/^[A-F0-9]{40}$/.test(policy.primaryFingerprint) || sha256(publicBytes)!==policy.publicKeySha256 ||
      !(Date.parse(policy.expiresAt)>Date.now()+30000) ||
      typeof challenge!=='string' || !challenge.startsWith('Reacon SDK signing qualification\n') || challenge.length>4096)
    throw new Error('Company signing policy, encrypted key and synthetic challenge required');
  const directory=await mkdtemp(join(tmpdir(),'reacon-maven-qualification-'));
  const signing=join(directory,'signing'), verification=join(directory,'verification');
  const env={PATH:'/usr/bin:/bin',HOME:directory,LANG:'C',LC_ALL:'C'};
  const gpg=async(home,args)=>{
    try {
      const result=await execute('/usr/bin/gpg',['--no-options','--homedir',home,'--batch','--no-tty',
        '--auto-key-locate','clear','--no-auto-key-retrieve',...args],{env,cwd:directory,timeout:30000,maxBuffer:65536});
      return result.stdout;
    } catch { throw new Error('Maven signing credential qualification failed; process details suppressed'); }
  };
  try {
    await mkdir(signing,{mode:0o700});await mkdir(verification,{mode:0o700});
    for(const [name,bytes] of Object.entries({'private.asc':privateBytes,'public.asc':publicBytes,'phrase':passphrase+'\n','challenge':challenge}))
      await writeFile(join(directory,name),bytes,{flag:'wx',mode:0o600});
    await gpg(signing,['--import',join(directory,'private.asc')]);
    const keys=(await gpg(signing,['--with-colons','--with-fingerprint','--list-secret-keys'])).split('\n').map(line=>line.split(':'));
    if(keys.filter(row=>row[0]==='sec').length!==1 || keys.some(row=>row[0]==='ssb') ||
      keys.find(row=>row[0]==='fpr')?.[9]!==policy.primaryFingerprint)throw new Error('Unexpected private signing identity');
    await gpg(signing,['--pinentry-mode','loopback','--passphrase-file',join(directory,'phrase'),
      '--armor','--digest-algo','SHA256','--local-user',policy.primaryFingerprint+'!',
      '--output',join(directory,'challenge.asc'),'--detach-sign',join(directory,'challenge')]);
    await gpg(verification,['--import-options','import-minimal','--import',join(directory,'public.asc')]);
    const status=await gpg(verification,['--status-fd','1','--verify',join(directory,'challenge.asc'),join(directory,'challenge')]);
    const result=parseMavenSignatureStatus(status,policy.primaryFingerprint);
    if(result.signerFingerprint!==policy.primaryFingerprint)throw new Error('Primary key signing required');
    return { ...result, publicKeySha256:policy.publicKeySha256, challengeSha256:sha256(Buffer.from(challenge)),
      signatureSha256:sha256(await readFile(join(directory,'challenge.asc'))),
      verifierBinarySha256:sha256(await readFile('/usr/bin/gpg')), signatureVerified:true,
      packageSigned:false, privateKeyRetainedInArtifact:false };
  } finally {
    privateBytes.fill(0);
    // Kill only these temporary keyrings' agents before removing their files.
    await Promise.allSettled([signing,verification].map(home=>execute('/usr/bin/gpgconf',['--homedir',home,'--kill','all'],{env,timeout:10000})));
    await rm(directory,{recursive:true,force:true});
  }
}
