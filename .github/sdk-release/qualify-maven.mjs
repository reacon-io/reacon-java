import { readFile, writeFile, mkdtemp, rm, mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { randomUUID, createHash } from 'node:crypto';
import { getIDToken, setSecret } from '@actions/core';
import { centralFileCredentials } from './central-credentials.mjs';
import { qualifyCentralAuthentication, qualifyMavenSigningKey } from './maven-credential-qualification.mjs';

const configuration=JSON.parse(await readFile(new URL('./configuration.json',import.meta.url)));
const policy=JSON.parse(await readFile(new URL('./maven-signing-key.json',import.meta.url)));
const names=['MAVEN_CENTRAL_CREDENTIALS','MAVEN_GPG_PRIVATE_KEY','MAVEN_GPG_PASSPHRASE'];
const values=Object.fromEntries(names.map(name=>[name,process.env[name]]));
for(const name of names){
  delete process.env[name];
  if(typeof values[name]!=='string' || !values[name])throw new Error(`Missing protected release secret: ${name}`);
  setSecret(values[name]);
}
const directory=await mkdtemp(join(tmpdir(),'reacon-central-credential-'));
try {
  const path=join(directory,'credentials.json');await writeFile(path,values.MAVEN_CENTRAL_CREDENTIALS,{flag:'wx',mode:0o600});
  const getCredentials=centralFileCredentials({path,configuration,environment:process.env,
    maskCredential:setSecret,tokenProvider:()=>getIDToken()});
  const credential=await getCredentials({registry:'maven-central',family:configuration.family,namespace:'io.reacon',
    repository:configuration.repository,workflow:'publish.yml',environment:'release'});
  const authentication=await qualifyCentralAuthentication({credentials:credential});
  const challenge=`Reacon SDK signing qualification\n${credential.workerId}\n${process.env.GITHUB_SHA}\n${randomUUID()}\n`;
  const signing=await qualifyMavenSigningKey({privateKey:values.MAVEN_GPG_PRIVATE_KEY,passphrase:values.MAVEN_GPG_PASSPHRASE,
    publicKey:await readFile(new URL('./reacon-sdk-signing-public.asc',import.meta.url)),policy,challenge});
  const report={formatVersion:1,kind:'sdk-maven-credential-qualification',observedAt:new Date().toISOString(),
    family:configuration.family,repository:configuration.repository,sourceCommit:process.env.GITHUB_SHA,
    workflow:'publish.yml',environment:'release',workerId:credential.workerId,identityClaimsSha256:credential.identityClaimsSha256,
    workflowSha256:createHash('sha256').update(await readFile('.github/workflows/publish.yml')).digest('hex'),
    authentication,signing,packagePublished:false,publishable:false};
  await mkdir('sdk-release-results',{recursive:true});
  await writeFile('sdk-release-results/maven.json',JSON.stringify(report,null,2)+'\n');
  console.log(`Verified Central authentication and primary signing key for ${configuration.repository}; no package upload.`);
} finally {
  for(const name of names)values[name]=null;
  await rm(directory,{recursive:true,force:true});
}
