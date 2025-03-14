function normalizeCoinId(coinId) {
  coinId = coinId.toLowerCase()
  const replaceSubStrings = ['asset#', 'coingecko#', 'coingecko:', 'ethereum:']
  const replaceSubStringLengths = replaceSubStrings.map(str => str.length)
  for (let i = 0; i < replaceSubStrings.length; i++) {
    const subStr = replaceSubStrings[i]
    const subStrLength = replaceSubStringLengths[i]
    if (coinId.startsWith(subStr))
      coinId = coinId.slice(subStrLength)

  }
  coinId = coinId.replace(/\//g, ':')
  if (coinId.length === 75 & coinId.startsWith('starknet:'))
    coinId = coinId.replace('0x0', '0x')
  return coinId
}

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function parsePidMapping(pidInput) {
  let inputArray = [];
  if (typeof pidInput === 'string') {
    inputArray = pidInput.split(',').map(p => p.trim()).filter(Boolean);
  } else if (Array.isArray(pidInput)) {
    inputArray = pidInput.map(p => p.trim()).filter(Boolean);
  }
  if (inputArray.length > 100) {
    throw new Error("Maximum of 100 PID tokens allowed.");
  }
  const mapping = {};
  const normalizedPids = inputArray.map(pid => {
    const normalized = normalizeCoinId(pid);
    mapping[pid] = normalized;
    return normalized;
  });
  return { mapping, normalizedPids };
}

function getEffectivePids(originalPid, mapping, metadata) {
  const normalized = mapping[originalPid];
  let candidates = [];
  const meta = metadata[originalPid];
  if (meta && Array.isArray(meta.redirects) && meta.redirects.length > 0) {
    candidates = [...meta.redirects];
  }
  candidates.push(normalized);
  return [...new Set(candidates)];
}


module.exports = {
  normalizeCoinId,
  sleep,
  parsePidMapping,
  getEffectivePids
}