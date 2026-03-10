const fs = require('fs');
const content = fs.readFileSync('main.py', 'utf8');

const old_text = `    markets = (
        db.query(Market).join(Token)
        .filter(Token.price > 0.03, Token.price < 0.97)
        .distinct().limit(500).all()
    )`;

const new_text = `    markets = (
        db.query(Market).join(Token)
        .filter(Token.price > 0.03, Token.price < 0.97)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .distinct().limit(500).all()
    )`;

if (content.includes(old_text)) {
    fs.writeFileSync('main.py', content.replace(old_text, new_text), 'utf8');
    console.log('OK! Filtro de mercados expirados adicionado!');
} else {
    console.log('Texto nao encontrado. Verificando o que tem no arquivo...');
    const idx = content.indexOf('.distinct().limit(500).all()');
    console.log('Contexto ao redor de .distinct().limit(500):', content.slice(Math.max(0, idx-200), idx+50));
}
