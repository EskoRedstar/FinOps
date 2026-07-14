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
TEFAS_FUND_CODE = os.environ.get("TEFAS_FUND_CODE", "YPT")  # Yapı Kredi Para Piyasası Fonu

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


# ============================================================
# Kaynak entegrasyonları
# ============================================================

async def fetch_btc(client: httpx.AsyncClient):
    try:
        r = await client.get("https://api.binance.com/api/v3/ticker/24hr", params={"symbol": "BTCUSDT"}, timeout=10)
        r.raise_for_status()
        data = r.json()
        _set("BTC", float(data["lastPrice"]), "USD", float(data["priceChangePercent"]), "Binance")
    except Exception as e:
        _mark_stale("BTC", str(e))


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

        if gold:
            price = float(str(gold.get("Selling", gold.get("Satış", 0))).replace(",", "."))
            change = float(str(gold.get("Change", gold.get("Değişim", 0))).replace(",", ".").replace("%", ""))
            _set("XAU", price, "TRY", change, "Truncgil")
        else:
            _mark_stale("XAU", "Truncgil yanıtında gram-altın bulunamadı")

        if silver:
            price = float(str(silver.get("Selling", silver.get("Satış", 0))).replace(",", "."))
            change = float(str(silver.get("Change", silver.get("Değişim", 0))).replace(",", ".").replace("%", ""))
            _set("XAG", price, "TRY", change, "Truncgil")
        else:
            _mark_stale("XAG", "Truncgil yanıtında gümüş bulunamadı")
    except Exception as e:
        _mark_stale("XAU", str(e))
        _mark_stale("XAG", str(e))


async def fetch_tefas(client: httpx.AsyncClient, fund_code: str = TEFAS_FUND_CODE):
    """TEFAS fiyatı günde bir kez (gün sonunda) açıklanır; bu fonksiyon son
    yayınlanan değeri çeker."""
    try:
        today = datetime.now().strftime("%d.%m.%Y")
        r = await client.post(
            "https://www.tefas.gov.tr/api/DB/BindHistoryInfo",
            data={"fontip": "YAT", "fonkod": fund_code, "bastarih": today, "bittarih": today},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=15,
        )
        r.raise_for_status()
        rows = r.json().get("data", [])
        if not rows:
            # Bugün henüz yayınlanmadıysa (piyasa kapanmadan) son 5 günü dene
            from datetime import timedelta
            past = (datetime.now() - timedelta(days=5)).strftime("%d.%m.%Y")
            r = await client.post(
                "https://www.tefas.gov.tr/api/DB/BindHistoryInfo",
                data={"fontip": "YAT", "fonkod": fund_code, "bastarih": past, "bittarih": today},
                timeout=15,
            )
            rows = r.json().get("data", [])
        if not rows:
            raise ValueError("TEFAS'tan veri dönmedi")
        rows.sort(key=lambda x: x.get("TARIH", ""))
        latest, prev = rows[-1], (rows[-2] if len(rows) > 1 else rows[-1])
        price = float(latest["FIYAT"])
        prev_price = float(prev["FIYAT"]) or price
        change_pct = ((price - prev_price) / prev_price) * 100 if prev_price else 0
        _set("YPT", price, "TRY", change_pct, "TEFAS")
    except Exception as e:
        _mark_stale("YPT", str(e))


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
        await fetch_tefas(c)


@app.on_event("startup")
async def on_startup():
    # açılışta bir kez hepsini çek, sonra periyodik devam et
    await asyncio.gather(job_btc(), job_stocks(), job_metals(), job_tefas())

    scheduler.add_job(job_btc, "interval", minutes=1, id="btc")
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
    jobs = {"BTC": job_btc, "MSTR": job_stocks, "TMQ": job_stocks, "XAU": job_metals, "XAG": job_metals, "YPT": job_tefas}
    if symbol not in jobs:
        raise HTTPException(404, f"'{symbol}' tanınmıyor.")
    await jobs[symbol]()
    return CACHE.get(symbol, {"status": "denendi, sonuç önbellekte yok"})
