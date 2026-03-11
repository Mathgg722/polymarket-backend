const fs = require('fs');
let content = fs.readFileSync('main.py', 'utf8');

const code = `

# ═══════════════════════════════════════════════════════════════════════════
# RECOMENDAÇÕES — Cruza Game Theory + MASTER + Anomalias + Kelly Criterion
# ═══════════════════════════════════════════════════════════════════════════

def _kelly(prob: float, odds: float, bankroll: float = 500, frac: float = 0.5) -> float:
    """
    Kelly Criterion fracionado (50% do Kelly completo = moderado).
    prob = probabilidade estimada de ganho (0-1)
    odds = retorno decimal (ex: se price=20c, odds = 100/20 = 5.0)
    Retorna valor em R$ a apostar.
    """
    if odds <= 1 or prob <= 0:
        return 0.0
    kelly = (prob * odds - 1) / (odds - 1)
    kelly_frac = kelly * frac  # moderado
    kelly_pct = max(0.03, min(kelly_frac, 0.10))  # clamp 3%-10%
    return round(bankroll * kelly_pct, 2)


def _score_confianca(fontes: list) -> dict:
    """Combina scores de múltiplas fontes em score único."""
    if not fontes:
        return {"score": 0, "nivel": "FRACA"}
    score = sum(f.get("score", 0) for f in fontes) / len(fontes)
    # Bônus por convergência
    if len(fontes) >= 3:
        score = min(score * 1.25, 100)
    elif len(fontes) == 2:
        score = min(score * 1.10, 100)
    score = round(score, 1)
    nivel = "🔴 FORTE" if score >= 75 else "🟠 ALTA" if score >= 60 else "🟡 MÉDIA" if score >= 45 else "🔵 FRACA"
    return {"score": score, "nivel": nivel, "num_fontes": len(fontes)}


@app.get("/recomendacoes")
async def get_recomendacoes(
    bankroll: float = Query(500.0),
    limite: int = Query(5, ge=1, le=10),
    db: Session = Depends(get_db),
):
    """
    Motor de Recomendações Unificado.
    Cruza Game Theory (Jiang) + MASTER (AAAI-2024) + Anomalias/Ineficiências.
    Aplica Kelly Criterion fracionado para calcular valor a apostar.
    """
    now = datetime.utcnow()
    mercados_map = {}  # slug -> dados consolidados

    # ── 1. GAME THEORY ────────────────────────────────────────────────────────
    try:
        markets_gt = (
            db.query(Market).join(Token)
            .filter(Token.price > 0.03, Token.price < 0.97)
            .filter((Market.end_date == None) | (Market.end_date > now))
            .distinct().limit(500).all()
        )
        for market in markets_gt:
            preds = _match_gt(market.question, market.market_slug or "")
            if not preds:
                continue
            pred = preds[0]
            prob_jiang = pred.get("prob_base", 70) / 100
            for token in market.tokens:
                if (token.outcome or "").upper() != "YES":
                    continue
                price = token.price
                if price < 0.03 or price > 0.95:
                    continue
                edge = round((prob_jiang - price) * 100, 1)
                if edge < 10:
                    continue
                slug = market.market_slug or market.question[:40]
                if slug not in mercados_map:
                    mercados_map[slug] = {
                        "market": market.question,
                        "slug": slug,
                        "url": f"https://polymarket.com/event/{slug}",
                        "price": round(price * 100, 1),
                        "fontes": [],
                        "acao": "COMPRAR YES",
                        "outcome": "YES",
                    }
                mercados_map[slug]["fontes"].append({
                    "nome": "Game Theory (Jiang)",
                    "icone": "♟️",
                    "score": min(50 + edge, 90),
                    "prob": round(prob_jiang * 100, 1),
                    "edge": edge,
                    "detalhe": pred.get("tema", "Análise geopolítica"),
                    "citacao": pred.get("citacao_curta", ""),
                })
    except Exception as e:
        print(f"[REC] Game Theory erro: {e}")

    # ── 2. MASTER ─────────────────────────────────────────────────────────────
    try:
        markets_m = (
            db.query(Market).join(Token)
            .filter(Token.price > 0.05, Token.price < 0.95)
            .filter((Market.end_date == None) | (Market.end_date > now))
            .distinct().limit(400).all()
        )
        for market in markets_m:
            try:
                token_prices = {}
                for token in market.tokens:
                    snaps = (
                        db.query(Snapshot)
                        .filter(Snapshot.token_id == token.token_id)
                        .order_by(Snapshot.timestamp.desc())
                        .limit(50).all()
                    )
                    if len(snaps) < 10:
                        continue
                    token_prices[token.token_id] = [s.price for s in snaps]

                for token in market.tokens:
                    if (token.outcome or "").upper() != "YES":
                        continue
                    prices = token_prices.get(token.token_id, [])
                    if len(prices) < 10:
                        continue
                    price = token.price
                    if price < 0.05 or price > 0.95:
                        continue
                    vol = _master_volatility(prices[:20])
                    mode = _master_gating(vol)
                    analysis = _master_intra_analysis(prices, price) if mode == "INTRA" else _master_inter_analysis(token_prices, token.token_id, price)
                    if analysis["confidence"] < 0.30 or analysis["signal"] == "NEUTRAL":
                        continue
                    predicted = analysis.get("predicted_price", price * 100)
                    edge = round(predicted - price * 100, 1)
                    if abs(edge) < 1:
                        continue
                    master_score = round(analysis["confidence"] * 50 + min(abs(edge) * 2, 30) + (20 if mode == "INTER" else 10), 1)
                    if master_score < 40:
                        continue
                    slug = market.market_slug or market.question[:40]
                    if slug not in mercados_map:
                        mercados_map[slug] = {
                            "market": market.question,
                            "slug": slug,
                            "url": f"https://polymarket.com/event/{slug}",
                            "price": round(price * 100, 1),
                            "fontes": [],
                            "acao": "COMPRAR YES" if analysis["signal"] == "UP" else "COMPRAR NO",
                            "outcome": "YES",
                        }
                    mercados_map[slug]["fontes"].append({
                        "nome": "MASTER (AAAI-2024)",
                        "icone": "🧬",
                        "score": master_score,
                        "prob": round(predicted, 1),
                        "edge": edge,
                        "detalhe": f"Modo {mode} · Vol {round(vol*100,2)}%",
                        "citacao": "",
                    })
            except:
                continue
    except Exception as e:
        print(f"[REC] MASTER erro: {e}")

    # ── 3. ANOMALIAS / INEFICIÊNCIAS ──────────────────────────────────────────
    try:
        tokens_all = (
            db.query(Token).join(Market)
            .filter(Token.price > 0.03, Token.price < 0.97)
            .filter((Market.end_date == None) | (Market.end_date > now))
            .limit(300).all()
        )
        for token in tokens_all:
            try:
                snaps = (
                    db.query(Snapshot)
                    .filter(Snapshot.token_id == token.token_id)
                    .order_by(Snapshot.timestamp.desc())
                    .limit(30).all()
                )
                if len(snaps) < 10:
                    continue
                prices = [s.price for s in snaps]
                mean_p = sum(prices) / len(prices)
                current = token.price
                deviation = abs(current - mean_p)
                if deviation < 0.05:
                    continue
                # Anomalia detectada
                direction = "UP" if current < mean_p else "DOWN"
                edge = round(deviation * 100, 1)
                anom_score = min(40 + edge * 2, 85)
                market = token.market
                if not market:
                    continue
                slug = market.market_slug or market.question[:40]
                if (token.outcome or "").upper() != "YES":
                    continue
                if slug not in mercados_map:
                    mercados_map[slug] = {
                        "market": market.question,
                        "slug": slug,
                        "url": f"https://polymarket.com/event/{slug}",
                        "price": round(current * 100, 1),
                        "fontes": [],
                        "acao": "COMPRAR YES" if direction == "UP" else "COMPRAR NO",
                        "outcome": "YES",
                    }
                mercados_map[slug]["fontes"].append({
                    "nome": "Anomalia de Preço",
                    "icone": "📡",
                    "score": anom_score,
                    "prob": round(mean_p * 100, 1),
                    "edge": edge,
                    "detalhe": f"Desvio {edge}% da média histórica",
                    "citacao": "",
                })
            except:
                continue
    except Exception as e:
        print(f"[REC] Anomalias erro: {e}")

    # ── 4. CONSOLIDAR + KELLY ─────────────────────────────────────────────────
    resultados = []
    for slug, dado in mercados_map.items():
        fontes = dado["fontes"]
        if not fontes:
            continue
        conf = _score_confianca(fontes)
        if conf["score"] < 35:
            continue
        price_pct = dado["price"]
        price_dec = price_pct / 100
        odds = round(1 / price_dec, 2) if price_dec > 0 else 1
        prob_estimada = min((conf["score"] / 100) * 0.85 + 0.10, 0.92)
        apostar_r = _kelly(prob_estimada, odds, bankroll)
        if apostar_r < 5:
            continue
        # Razão da recomendação
        nomes_fontes = [f["icone"] + " " + f["nome"] for f in fontes]
        convergencia = len(fontes)
        razao = f"{convergencia} fonte{'s' if convergencia > 1 else ''} convergem"
        if convergencia >= 2:
            razao = "✦ CONVERGÊNCIA: " + " + ".join(f["icone"] for f in fontes)

        resultados.append({
            "market": dado["market"],
            "slug": slug,
            "url": dado["url"],
            "acao": dado["acao"],
            "price": price_pct,
            "odds": odds,
            "confianca": conf,
            "prob_estimada": round(prob_estimada * 100, 1),
            "apostar_r": apostar_r,
            "apostar_pct": round((apostar_r / bankroll) * 100, 1),
            "retorno_esperado": round(apostar_r * (odds - 1) * prob_estimada - apostar_r * (1 - prob_estimada), 2),
            "fontes": fontes,
            "razao": razao,
            "convergencia": convergencia,
            "analisado_em": now.isoformat(),
        })

    # Ordenar: primeiro por convergência, depois por score
    resultados.sort(key=lambda x: (x["convergencia"], x["confianca"]["score"]), reverse=True)
    top = resultados[:limite]

    total_investir = sum(r["apostar_r"] for r in top)
    retorno_esperado = sum(r["retorno_esperado"] for r in top)

    return {
        "titulo": "Recomendações do Sistema",
        "descricao": "Cruzamento de Game Theory + MASTER + Anomalias com Kelly Criterion",
        "bankroll": bankroll,
        "total_mercados_analisados": len(mercados_map),
        "resumo": {
            "total_recomendacoes": len(top),
            "total_investir_r": round(total_investir, 2),
            "retorno_esperado_r": round(retorno_esperado, 2),
            "roi_esperado_pct": round((retorno_esperado / total_investir * 100) if total_investir > 0 else 0, 1),
        },
        "recomendacoes": top,
        "gerado_em": now.isoformat(),
    }
`;

// Inserir antes do endpoint game-theory/mec ou no final
const insertPoint = '\n@app.get("/game-theory/mec")';
if (content.includes(insertPoint)) {
    content = content.replace(insertPoint, code + insertPoint);
    console.log('OK! Inserido antes de /game-theory/mec');
} else {
    content = content + code;
    console.log('OK! Inserido no final');
}

fs.writeFileSync('main.py', content, 'utf8');
console.log('Linhas totais:', content.split('\n').length);
