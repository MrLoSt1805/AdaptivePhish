"""FastAPI prediction service with aggregate Prometheus metrics."""

from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, Response, JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from adaptivephish.models.artifact import ModelArtifactError, predict_url, load_trusted_model
from adaptivephish.monitoring.metrics import PREDICTED_CLASSES


class PredictionRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    url: str = Field(min_length=1, max_length=16_384)


REQUESTS = Counter('adaptivephish_predictions_total', 'Prediction requests',
                   ['outcome'])
LATENCY = Histogram('adaptivephish_prediction_seconds', 'Prediction latency')

app = FastAPI(title='AdaptivePhish API', version='0.1.0')


@app.exception_handler(RequestValidationError)
async def validation_error(request, error):
    REQUESTS.labels(outcome='validation_failure').inc()
    return JSONResponse(status_code=422, content={'detail': 'invalid_prediction_input'})


@app.get('/ready')
def ready():
    try:
        bundle = load_trusted_model()
        return {'status': 'ready', 'model_version': bundle['model_version']}
    except ModelArtifactError:
        raise HTTPException(status_code=503, detail='model_unavailable') from None


@app.get('/model/info')
def model_info():
    try:
        bundle = load_trusted_model()
        return {key: bundle[key] for key in
                ('model_version', 'feature_schema_version', 'feature_names')}
    except ModelArtifactError:
        raise HTTPException(status_code=503, detail='model_unavailable') from None

HOME_PAGE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>AdaptivePhish</title>
  <style>
    :root { color-scheme: dark; --ink:#e9f4f1; --muted:#9bb3ad;
      --panel:#102521; --line:#28453f; --accent:#4ee0b5; --danger:#ff7b72; }
    * { box-sizing:border-box; }
    body { margin:0; min-height:100vh; font-family:Inter,Segoe UI,sans-serif;
      color:var(--ink); background:radial-gradient(circle at 80% 10%,#173d34,transparent 34%),#071512; }
    main { width:min(760px,calc(100% - 32px)); margin:auto; padding:72px 0; }
    .eyebrow { color:var(--accent); font-size:.78rem; font-weight:700;
      letter-spacing:.16em; text-transform:uppercase; }
    h1 { margin:.35rem 0 .7rem; font-size:clamp(2.5rem,8vw,5.5rem);
      letter-spacing:-.06em; line-height:.92; }
    .lead { max-width:620px; color:var(--muted); font-size:1.05rem; line-height:1.65; }
    section { margin-top:38px; padding:28px; border:1px solid var(--line);
      border-radius:18px; background:color-mix(in srgb,var(--panel) 92%,transparent);
      box-shadow:0 24px 80px #0007; }
    label { display:block; margin-bottom:10px; font-weight:650; }
    .row { display:flex; gap:10px; }
    input { flex:1; min-width:0; border:1px solid var(--line); border-radius:10px;
      padding:14px; color:var(--ink); background:#091b17; font:inherit; }
    input:focus { outline:2px solid var(--accent); outline-offset:2px; }
    button { border:0; border-radius:10px; padding:14px 20px; cursor:pointer;
      background:var(--accent); color:#062019; font-weight:800; }
    button:disabled { cursor:wait; opacity:.65; }
    #result { min-height:68px; margin-top:18px; padding:16px; border-radius:10px;
      background:#091b17; color:var(--muted); line-height:1.55; }
    .phish { color:var(--danger); } .benign { color:var(--accent); }
    .meta { display:flex; flex-wrap:wrap; gap:14px; margin-top:30px;
      color:var(--muted); font-size:.86rem; }
    .meta a { color:var(--ink); }
    @media (max-width:600px) { main{padding:42px 0}.row{flex-direction:column} }
  </style>
</head>
<body>
  <main>
    <div class="eyebrow">URL-only phishing research system</div>
    <h1>Adaptive<br>Phish</h1>
    <p class="lead">Analyze lexical URL patterns locally with the approved
      Phase 3 model. The service treats the address as inert text—it does not
      open, resolve, or visit the submitted URL.</p>
    <section aria-labelledby="checker-title">
      <h2 id="checker-title">Check a URL</h2>
      <form id="checker">
        <label for="url">URL text</label>
        <div class="row">
          <input id="url" name="url" type="text" required maxlength="16384"
            placeholder="https://example.com" autocomplete="off" spellcheck="false">
          <button id="submit" type="submit">Analyze</button>
        </div>
      </form>
      <div id="result" role="status" aria-live="polite">Ready for local analysis.</div>
    </section>
    <div class="meta"><span>Model: phase3-v1</span><span>Features: URL lexical v1</span>
      <a href="/docs">API documentation</a><a href="/health">Health</a></div>
  </main>
  <script>
    const form=document.querySelector('#checker'), result=document.querySelector('#result');
    const button=document.querySelector('#submit');
    form.addEventListener('submit',async(event)=>{
      event.preventDefault(); button.disabled=true; result.className='';
      result.textContent='Analyzing lexical features…';
      try {
        const response=await fetch('/predict',{method:'POST',headers:{'Content-Type':'application/json'},
          body:JSON.stringify({url:document.querySelector('#url').value})});
        if(!response.ok) throw new Error('unavailable');
        const data=await response.json(), probability=(data.phishing_probability*100).toFixed(1);
        result.className=data.predicted_label;
        result.textContent=`Prediction: ${data.predicted_label.toUpperCase()} · phishing probability ${probability}%`;
      } catch (_) { result.className='phish'; result.textContent='Prediction is unavailable. Check that the model artifact is present.'; }
      finally { button.disabled=false; }
    });
  </script>
</body>
</html>"""


@app.get('/', response_class=HTMLResponse, include_in_schema=False)
def home():
    return HTMLResponse(HOME_PAGE, headers={
        'Cache-Control': 'no-store',
        'Content-Security-Policy': "default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'",
        'X-Content-Type-Options': 'nosniff',
        'Referrer-Policy': 'no-referrer',
    })


@app.get('/health')
def health():
    return {'status': 'ok'}


@app.post('/predict')
def predict(request: PredictionRequest):
    try:
        with LATENCY.time():
            result = predict_url(request.url)
        REQUESTS.labels(outcome='success').inc()
        PREDICTED_CLASSES.labels(label=result['predicted_label'], version=result['model_version']).inc()
        return result
    except ModelArtifactError:
        REQUESTS.labels(outcome='failure').inc()
        raise HTTPException(status_code=503, detail='prediction_unavailable') from None


@app.get('/metrics')
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
