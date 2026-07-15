"""
fiyat_servisi.py
=================
ESKO için gerçek zamanlı fiyat servisi. Mock veri katmanının yerini alır.

Kaynaklar:
  - BTC/USD      → Binance public REST API (anahtar gerekmez)
  - MSTR, TMQ    → Finnhub (ücretsiz katman, API anahtarı gerekir)
  - Gram Altın/Gümüş → Truncgil API (anahtar gerekmez, TL bazlı)
  - YPT (TEFAS)  → TEFAS'ın kendi veri servisi (anahtar gerekmez, günde 1 güncellenir)

Kurulum:
    pip install -r requirements.txt
    cp .env.example .env   # sonra FINNHUB_API_KEY'i doldurun
    uvicorn fiyat_servisi:app --reload --port 8000

Frontend'e bağlama: esko-pwa/index.html içindeki PRICE_ENDPOINT değişkenine
bu servisin adresini yazın (örn. https://sizin-servisiniz.onrender.com/prices).
"""

from __future__ import annotations

import os
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("fiyat_servisi")

FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "")
FONOLOJI_API_KEY = os.environ.get("FONOLOJI_API_KEY", "")
TEFAS_FUND_CODE = os.environ.get("TEFAS_FUND_CODE", "YPT")  # Yapı Kredi Para Piyasası Fonu
HMG_FUND_CODE = os.environ.get("HMG_FUND_CODE", "HMG")      # HSBC Portföy Para Piyasası Fonu

app = FastAPI(title="ESKO Fiyat Servisi")

# Frontend farklı bir alan adında barınacağı için CORS açık.
# --- ÜRETİMDE DEĞİŞTİR: allow_origins=["https://sizin-domaininiz.com"] ile daraltın ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ============================================================
# Basit bellek-içi önbellek (tek instance için yeterli;
# çoklu instance'da Redis'e taşıyın)
# ============================================================
CACHE: dict[str, dict] = {}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _set(symbol: str, price: float, currency: str, change_pct: float, source: str):
    CACHE[symbol] = {
        "symbol": symbol,
        "price": round(price, 6),
        "currency": currency,
        "change_pct": round(change_pct, 3),
        "source": source,
        "updated_at": _now(),
        "stale": False,
    }


def _mark_stale(symbol: str, error: str):
    """Çağrı başarısız olursa son bilinen değeri koru ama 'stale' işaretle;
    hiçbir zaman veri uydurma."""
    if symbol in CACHE:
        CACHE[symbol]["stale"] = True
        CACHE[symbol]["error"] = error
    else:
        CACHE[symbol] = {"symbol": symbol, "price": None, "currency": None,
                          "change_pct": None, "source": None, "updated_at": _now(),
                          "stale": True, "error": error}
    log.warning(f"{symbol} güncellenemedi: {error}")


def _parse_try_number(raw) -> float:
    """Türkçe sayı formatını (binlik ayraç '.', ondalık ayraç ',') güvenle
    float'a çevirir. Örn: '6.082,74' -> 6082.74. '%' işaretini de temizler.
    Zaten standart formatta ('6082.74') gelen değerleri de doğru işler."""
    s = str(raw).strip().replace("%", "").replace(" ", "")
    if not s:
        raise ValueError("boş değer")
    if "," in s and "." in s:
        # '.' binlik ayraç, ',' ondalık ayraç (Türkçe format)
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    return float(s)


# ============================================================
# Kaynak entegrasyonları
# ============================================================

async def fetch_btc(client: httpx.AsyncClient):
    """Birincil kaynak CoinGecko; rate limit (429) veya hata durumunda
    Coinbase'in genel spot fiyat uç noktasına düşer. İkisi de anahtar
    gerektirmez ve Binance'in aksine coğrafi kısıtlama uygulamaz."""
    try:
        r = await client.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "bitcoin", "vs_currencies": "usd", "include_24hr_change": "true"},
            headers={"User-Agent": "EskoApp/1.0 (+https://esko.app)"},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        btc = data.get("bitcoin", {})
        price = btc.get("usd")
        change = btc.get("usd_24h_change")
        if price is None:
            raise ValueError(f"beklenmeyen yanıt: {data}")
        _set("BTC", float(price), "USD", float(change or 0), "CoinGecko")
        return
    except Exception as e:
        coingecko_error = str(e)

    # Yedek kaynak: Coinbase (change_pct vermez, son bilinen değişim korunur)
    try:
        r = await client.get(
            "https://api.coinbase.com/v2/prices/BTC-USD/spot",
            headers={"User-Agent": "EskoApp/1.0 (+https://esko.app)"},
            timeout=10,
        )
        r.raise_for_status()
        amount = r.json().get("data", {}).get("amount")
        if amount is None:
            raise ValueError("Coinbase yanıtında fiyat yok")
        prev_change = CACHE.get("BTC", {}).get("change_pct") or 0
        _set("BTC", float(amount), "USD", prev_change, "Coinbase (yedek)")
    except Exception as e2:
        _mark_stale("BTC", f"CoinGecko: {coingecko_error} | Coinbase: {e2}")


async def fetch_finnhub_quote(client: httpx.AsyncClient, symbol: str):
    if not FINNHUB_API_KEY:
        _mark_stale(symbol, "FINNHUB_API_KEY tanımlı değil")
        return
    try:
        r = await client.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": symbol, "token": FINNHUB_API_KEY}, timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        price = data.get("c")  # current price
        prev_close = data.get("pc")
        if not price or not prev_close:
            raise ValueError(f"beklenmeyen yanıt: {data}")
        change_pct = ((price - prev_close) / prev_close) * 100
        _set(symbol, price, "USD", change_pct, "Finnhub")
    except Exception as e:
        _mark_stale(symbol, str(e))


async def fetch_gold_silver(client: httpx.AsyncClient):
    try:
        r = await client.get("https://finans.truncgil.com/today.json", timeout=10)
        r.raise_for_status()
        data = r.json()

        gold = data.get("gram-altin") or data.get("GRA") or {}
        silver = data.get("gumus") or data.get("GUM") or {}
        usd = data.get("dolar") or data.get("USD") or {}

        def _price_field(d):
            for key in ("Selling", "Satış", "satis", "selling"):
                if key in d:
                    return d[key]
            return None

        def _change_field(d):
            for key in ("Change", "Değişim", "degisim", "change"):
                if key in d:
                    return d[key]
            return None

        if gold:
            raw_price = _price_field(gold)
            if raw_price is None:
                raise ValueError(f"gram-altın fiyat alanı bulunamadı: {list(gold.keys())}")
            price = _parse_try_number(raw_price)
            change = _parse_try_number(_change_field(gold) or 0)
            _set("XAU", price, "TRY", change, "Truncgil")
        else:
            _mark_stale("XAU", "Truncgil yanıtında gram-altın bulunamadı")

        if silver:
            raw_price = _price_field(silver)
            if raw_price is None:
                raise ValueError(f"gümüş fiyat alanı bulunamadı: {list(silver.keys())}")
            price = _parse_try_number(raw_price)
            change = _parse_try_number(_change_field(silver) or 0)
            _set("XAG", price, "TRY", change, "Truncgil")
        else:
            _mark_stale("XAG", "Truncgil yanıtında gümüş bulunamadı")

        # USD/TRY kurunu aynı yanıttan çek (ekstra istek yok)
        if usd:
            raw_usd = _price_field(usd)
            if raw_usd:
                _set("USDTRY", _parse_try_number(raw_usd), "TRY",
                     _parse_try_number(_change_field(usd) or 0), "Truncgil")
        else:
            await _fetch_usdtry_fallback(client)

    except Exception as e:
        _mark_stale("XAU", str(e))
        _mark_stale("XAG", str(e))
        await _fetch_usdtry_fallback(client)


async def _fetch_usdtry_fallback(client: httpx.AsyncClient):
    """Truncgil'de USD/TRY yoksa ExchangeRate-API'ye düşer (anahtar gerekmez)."""
    try:
        r = await client.get(
            "https://api.exchangerate-api.com/v4/latest/USD", timeout=10
        )
        r.raise_for_status()
        rate = r.json().get("rates", {}).get("TRY")
        if rate:
            prev = CACHE.get("USDTRY", {}).get("price") or rate
            change_pct = ((rate - prev) / prev) * 100 if prev else 0
            _set("USDTRY", float(rate), "TRY", change_pct, "ExchangeRate-API")
    except Exception as e:
        _mark_stale("USDTRY", str(e))


async def fetch_fund(client: httpx.AsyncClient, symbol: str, fund_code: str):
    """TEFAS fonları (YPT, HMG vb.) için Fonoloji API kullanılıyor
    (https://fonoloji.com/api-docs) — TEFAS'ın 2026'daki API değişikliğini
    kendi içinde çözen, güncel bakımı yapılan bir servis. Ücretsiz anahtar:
    https://fonoloji.com/kayit"""
    if not FONOLOJI_API_KEY:
        _mark_stale(symbol, "FONOLOJI_API_KEY tanımlı değil")
        return
    try:
        r = await client.get(
            f"https://fonoloji.com/v1/funds/{fund_code}",
            headers={"X-API-Key": FONOLOJI_API_KEY},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        fund = data.get("fund", {})
        price = fund.get("current_price")
        return_1d = fund.get("return_1d")
        if price is None:
            raise ValueError(f"beklenmeyen yanıt: {data}")
        change_pct = (return_1d or 0) * 100
        _set(symbol, float(price), "TRY", change_pct, "Fonoloji")
    except Exception as e:
        _mark_stale(symbol, str(e))


# ============================================================
# Zamanlayıcı — her kaynağın gerçek güncelleme sıklığına göre
# ============================================================
scheduler = AsyncIOScheduler()
_client: Optional[httpx.AsyncClient] = None


async def job_btc():
    async with httpx.AsyncClient() as c:
        await fetch_btc(c)

async def job_stocks():
    async with httpx.AsyncClient() as c:
        await asyncio.gather(fetch_finnhub_quote(c, "MSTR"), fetch_finnhub_quote(c, "TMQ"))

async def job_metals():
    async with httpx.AsyncClient() as c:
        await fetch_gold_silver(c)

async def job_tefas():
    async with httpx.AsyncClient() as c:
        await asyncio.gather(
            fetch_fund(c, "YPT", TEFAS_FUND_CODE),
            fetch_fund(c, "HMG", HMG_FUND_CODE),
        )


@app.on_event("startup")
async def on_startup():
    # açılışta bir kez hepsini çek, sonra periyodik devam et
    await asyncio.gather(job_btc(), job_stocks(), job_metals(), job_tefas())

    scheduler.add_job(job_btc, "interval", minutes=3, id="btc")  # CoinGecko ücretsiz katman rate limit'i için seyreltildi
    scheduler.add_job(job_stocks, "interval", minutes=2, id="stocks")   # Finnhub ücretsiz katman limiti düşünülerek
    scheduler.add_job(job_metals, "interval", minutes=5, id="metals")
    scheduler.add_job(job_tefas, "cron", hour=19, minute=0, id="tefas")  # TEFAS fiyatı akşam açıklanır
    scheduler.start()
    log.info("Zamanlayıcı başlatıldı.")


@app.on_event("shutdown")
async def on_shutdown():
    scheduler.shutdown()


# ============================================================
# API uç noktaları
# ============================================================

class PriceOut(BaseModel):
    symbol: str
    price: Optional[float]
    currency: Optional[str]
    change_pct: Optional[float]
    source: Optional[str]
    updated_at: str
    stale: bool
    error: Optional[str] = None


@app.get("/health")
async def health():
    return {"status": "ok", "time": _now()}


@app.get("/prices", response_model=dict[str, PriceOut])
async def get_all_prices():
    if not CACHE:
        raise HTTPException(503, "Veri henüz hazır değil, birkaç saniye sonra tekrar deneyin.")
    return CACHE


@app.get("/prices/{symbol}", response_model=PriceOut)
async def get_price(symbol: str):
    symbol = symbol.upper()
    if symbol not in CACHE:
        raise HTTPException(404, f"'{symbol}' için veri bulunamadı.")
    return CACHE[symbol]


@app.post("/prices/{symbol}/refresh")
async def force_refresh(symbol: str):
    """Belirli bir sembolü zamanlamayı beklemeden hemen tazeler (manuel test için)."""
    symbol = symbol.upper()
    jobs = {"BTC": job_btc, "MSTR": job_stocks, "TMQ": job_stocks, "XAU": job_metals, "XAG": job_metals, "USDTRY": job_metals, "YPT": job_tefas, "HMG": job_tefas}
    if symbol not in jobs:
        raise HTTPException(404, f"'{symbol}' tanınmıyor.")
    await jobs[symbol]()
    return CACHE.get(symbol, {"status": "denendi, sonuç önbellekte yok"})
