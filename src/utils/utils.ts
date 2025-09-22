const toInstId = (token: string): string => {
  const t = token.trim();
  if (!t) return '';
  const u = t.toUpperCase();
  return u.includes('-') ? u : `${u}-USDT-SWAP`;
};

export const parseSymbolsFromEnv = (): string[] => {
  const raw =
    process.env.OKX_ASSETS ??
    process.env.OKX_SYMBOLS ??
    'btc,eth,doge,ltc,shib,pump,wlfi,xpl';
  const list = raw
    .split(',')
    .map((s) => toInstId(s))
    .filter(Boolean);
  return Array.from(new Set(list));
};
