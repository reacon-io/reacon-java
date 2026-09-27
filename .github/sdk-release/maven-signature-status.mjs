const fingerprint = value => /^(?:[A-F0-9]{40}|[A-F0-9]{64})$/.test(value ?? '');

export function parseMavenSignatureStatus(status, primaryFingerprint) {
  const lines = status.trim().split(/\r?\n/).filter(Boolean);
  if (!fingerprint(primaryFingerprint) || lines.some(line => !line.startsWith('[GNUPG:] '))) throw new Error('Invalid GPG status stream');
  const records = lines.map(line => line.slice(9).split(' '));
  const valid = records.filter(fields => fields[0] === 'VALIDSIG');
  if (valid.length !== 1 || records.filter(fields => fields[0] === 'GOODSIG').length !== 1 ||
      records.some(fields => ['BADSIG', 'ERRSIG', 'EXPSIG', 'EXPKEYSIG', 'REVKEYSIG', 'KEYREVOKED',
        'KEYEXPIRED', 'SIGEXPIRED', 'FAILURE', 'ERROR', 'NODATA', 'NO_PUBKEY'].includes(fields[0]))) throw new Error('Invalid or expired Maven signature');
  const fields = valid[0];
  // VALIDSIG: signer, date, timestamp, expiry, version, reserved, public-key
  // algorithm, hash algorithm, signature class, optional primary fingerprint.
  if (![10, 11].includes(fields.length) || !fingerprint(fields[1]) ||
      (fields[10] ?? fields[1]) !== primaryFingerprint || !['8', '9', '10'].includes(fields[8]) || fields[9] !== '00') throw new Error('Maven signature does not match company policy');
  return { primaryFingerprint, signerFingerprint: fields[1], hashAlgorithm: fields[8] };
}

