const fs = require('fs');
let content = fs.readFileSync('main.py', 'utf8');

// Fix 1: Aumentar min_edge padrão de 5 para 15 para reduzir matches ruins
const old1 = `    min_edge: float = Query(5.0),`;
const new1 = `    min_edge: float = Query(15.0),`;

// Fix 2: Melhorar _match_gt para exigir score mínimo de 2 matches E filtrar mercados absurdos
const old2 = `def _match_gt(question: str, slug: str) -> list:
    text = (question + " " + slug).lower()
    scores: dict = {}
    for kw, ids in _GT_INDEX.items():
        if kw in text:
            for pid in ids:
                scores[pid] = scores.get(pid, 0) + 1
    if not scores:
        return []
    matched = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return [_GT_MAP[pid] for pid, _ in matched if pid in _GT_MAP]`;

const new2 = `# Palavras que indicam mercado irrelevante para análise geopolítica/econômica
_NOISE_PATTERNS = [
    "jesus", "christ", "god", "alien", "ufo", "bigfoot", "unicorn",
    "celebrity", "kardashian", "taylor swift", "kanye", "bieber",
    "will smith", "oscars slap", "nicki minaj",
]

def _match_gt(question: str, slug: str) -> list:
    text = (question + " " + slug).lower()
    
    # Filtrar mercados claramente irrelevantes
    if any(noise in text for noise in _NOISE_PATTERNS):
        return []
    
    scores: dict = {}
    for kw, ids in _GT_INDEX.items():
        if kw in text:
            for pid in ids:
                scores[pid] = scores.get(pid, 0) + 1
    if not scores:
        return []
    
    # Exige score mínimo de 2 para evitar matches por 1 palavra genérica
    filtered = {pid: s for pid, s in scores.items() if s >= 2}
    if not filtered:
        # Se nenhum tem score 2+, pega só o melhor se tiver score 1 E keyword forte
        best_pid = max(scores, key=lambda x: scores[x])
        best_pred = _GT_MAP.get(best_pid)
        if best_pred:
            # Verifica se pelo menos 1 keyword do tema está no texto
            core_kws = best_pred.get("keywords", [])[:5]
            if any(kw in text for kw in core_kws):
                filtered = {best_pid: scores[best_pid]}
            else:
                return []
    
    matched = sorted(filtered.items(), key=lambda x: x[1], reverse=True)
    return [_GT_MAP[pid] for pid, _ in matched if pid in _GT_MAP]`;

let changed = 0;

if (content.includes(old1)) {
    content = content.replace(old1, new1);
    changed++;
    console.log('OK: min_edge aumentado para 15');
} else {
    console.log('AVISO: min_edge nao encontrado');
}

if (content.includes(old2)) {
    content = content.replace(old2, new2);
    changed++;
    console.log('OK: _match_gt melhorado com filtros');
} else {
    console.log('AVISO: _match_gt nao encontrado, verificando...');
    const idx = content.indexOf('def _match_gt');
    if (idx >= 0) {
        console.log('Contexto:', content.slice(idx, idx + 300));
    }
}

if (changed > 0) {
    fs.writeFileSync('main.py', content, 'utf8');
    console.log(`\nTotal de fixes aplicados: ${changed}/2`);
} else {
    console.log('Nenhuma mudanca aplicada!');
}
