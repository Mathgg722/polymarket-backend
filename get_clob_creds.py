# ============================================================
# PolySignal — Gerar credenciais CLOB L2
# Roda UMA VEZ localmente para pegar POLY_API_KEY,
# POLY_SECRET, POLY_PASSPHRASE
#
# Como usar:
#   1) pip install py-clob-client
#   2) set PRIVATE_KEY=0xSUA_CHAVE_AQUI  (no terminal, NÃO no código!)
#   3) python get_clob_creds.py
#   4) Copie os valores para o Railway
# ============================================================

import os
import sys

def main():
    pk = os.environ.get("PRIVATE_KEY", "")

    if not pk:
        print("❌ PRIVATE_KEY não encontrada no ambiente!")
        print()
        print("Configure antes de rodar:")
        print("  Windows PowerShell: $env:PRIVATE_KEY = '0xSUA_CHAVE'")
        print("  Linux/Mac:         export PRIVATE_KEY='0xSUA_CHAVE'")
        sys.exit(1)

    if not pk.startswith("0x"):
        pk = "0x" + pk

    print(f"🔑 Usando chave: {pk[:6]}...{pk[-4:]} (omitido por segurança)")
    print("⏳ Conectando ao CLOB...")

    try:
        from py_clob_client.client import ClobClient
    except ImportError:
        print("❌ py-clob-client não instalado!")
        print("   pip install py-clob-client")
        sys.exit(1)

    try:
        client = ClobClient(
            host="https://clob.polymarket.com",
            chain_id=137,
            key=pk,
        )
        creds = client.create_or_derive_api_creds()

        print("\n✅ CREDENCIAIS GERADAS COM SUCESSO!")
        print("=" * 50)
        print(f"POLY_API_KEY    = {creds.api_key}")
        print(f"POLY_SECRET     = {creds.api_secret}")
        print(f"POLY_PASSPHRASE = {creds.api_passphrase}")
        print("=" * 50)
        print()
        print("📋 Copie esses valores para as variáveis do Railway:")
        print("   polymarket-backend → Variables")
        print()
        print("⚠️  NUNCA coloque esses valores no código ou GitHub!")

    except Exception as e:
        print(f"❌ Erro ao gerar credenciais: {e}")
        print()
        print("Verifique:")
        print("  - PRIVATE_KEY está correto (com 0x no início)")
        print("  - Você tem conexão com a internet")
        print("  - A carteira já usou o Polymarket ao menos uma vez")
        sys.exit(1)


if __name__ == "__main__":
    main()
