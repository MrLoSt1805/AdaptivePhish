"""Run the local AdaptivePhish API without development auto-reload."""

import uvicorn


if __name__ == '__main__':
    uvicorn.run('adaptivephish.api.app:app', host='127.0.0.1', port=8000,
                reload=False, access_log=False)
