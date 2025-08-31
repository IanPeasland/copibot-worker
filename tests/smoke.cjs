const assert = (c,m) => { if(!c){ console.error(m); process.exit(1);} };
const RX_YES_CONFIRM = /\b(s[ií]|sí|si|claro|va|dale|correcto|ok|afirmativo|hazlo|agr[eé]ga(lo)?|añade|m[eé]te|pon(lo)?)\b/i;
assert(RX_YES_CONFIRM.test('hola, si agrégalo por favor'), 'No detectó "si agrégalo"');
assert(/\b(con|con factura|factura)\b/i.test('con'), 'No detectó "con"');
assert(/\b(sin|sin factura|no)\b/i.test('sin'), 'No detectó "sin"');
console.log('Smoke OK');
