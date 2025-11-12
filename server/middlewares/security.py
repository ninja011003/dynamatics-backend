from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path.startswith("/docs"):
            response = await call_next(request)
            response.headers["Content-Security-Policy"] = (
                "default-src 'self'; "
                "img-src 'self' data: https://fastapi.tiangolo.com; "
                "script-src 'self' https://cdn.jsdelivr.net 'unsafe-inline'; "
                "style-src 'self' 'unsafe-inline';"
            )
            return response

        response: Response = await call_next(request)

        # Strict-Transport-Security (1 year, preload, include subdomains)
        response.headers["Strict-Transport-Security"] = (
            "max-age=31536000; includeSubDomains; preload"
        )

        # Prevent MIME type sniffing
        response.headers["X-Content-Type-Options"] = "nosniff"

        # Prevent clickjacking
        response.headers["X-Frame-Options"] = "DENY"

        # Legacy XSS filter (modern browsers prefer CSP)
        response.headers["X-XSS-Protection"] = "1; mode=block"

        # Content Security Policy (restrict inline + external sources)
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; "
            "font-src 'self'; "
            "object-src 'none'; "
            "base-uri 'self'; "
            "frame-ancestors 'none'; "
            "form-action 'self'"
        )

        # Permissions Policy (restrict sensitive APIs)
        response.headers["Permissions-Policy"] = (
            "camera=(), microphone=(), geolocation=()"
        )

        # Referrer Policy
        response.headers["Referrer-Policy"] = "no-referrer-when-downgrade"

        # COEP, COOP, CORP (isolation for cross-origin stuff)
        response.headers["Cross-Origin-Embedder-Policy"] = "require-corp"
        response.headers["Cross-Origin-Opener-Policy"] = "same-origin"
        response.headers["Cross-Origin-Resource-Policy"] = "same-origin"

        # DNS Prefetch Control
        response.headers["X-DNS-Prefetch-Control"] = "off"

        # Uvicorn apparently adds itself in lower case like this : "server"
        if "server" in response.headers:
            del response.headers["server"]

        response.headers["Server"] = "secure"

        return response
