const fs = require('fs');
let content = fs.readFileSync('main.py', 'utf8');

// Baixar confiança mínima de 0.45 para 0.30
const old1 = 'if analysis["confidence"] < 0.45:';
const new1 = 'if analysis["confidence"] < 0.30:';

// Baixar edge mínimo de 2 para 1
const old2 = 'if abs(edge) < 2:';
const new2 = 'if abs(edge) < 1:';

// Incluir NEUTRAL com edge alto
const old3 = '                if analysis["signal"] == "NEUTRAL":\n                    continue';
const new3 = '                if analysis["signal"] == "NEUTRAL" and abs(analysis.get("predicted_price", current*100) - current*100) < 3:\n                    continue';

// Aumentar limite de mercados analisados de 200 para 400
const old4 = '.filter((Market.end_date == None) | (Market.end_date > now))\n        .distinct().limit(200).all()';
const new4 = '.filter((Market.end_date == None) | (Market.end_date > now))\n        .distinct().limit(400).all()';

let changed = 0;

if (content.includes(old1)) { content = content.replace(old1, new1); changed++; console.log('OK: confianca minima 0.45 -> 0.30'); }
else console.log('AVISO: confianca minima nao encontrada');

if (content.includes(old2)) { content = content.replace(old2, new2); changed++; console.log('OK: edge minimo 2 -> 1'); }
else console.log('AVISO: edge minimo nao encontrado');

if (content.includes(old3)) { content = content.replace(old3, new3); changed++; console.log('OK: NEUTRAL filter relaxado'); }
else console.log('AVISO: NEUTRAL filter nao encontrado');

if (content.includes(old4)) { content = content.replace(old4, new4); changed++; console.log('OK: limite mercados 200 -> 400'); }
else console.log('AVISO: limite mercados nao encontrado');

fs.writeFileSync('main.py', content, 'utf8');
console.log('Total fixes:', changed);
