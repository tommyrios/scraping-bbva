from dataclasses import dataclass
from typing import Dict, List, Any


@dataclass(frozen=True)
class Seccion:
    key: str
    titulo: str


class ReporteUI:

    LOGO_CID = "cid:bbva_logo"

    def __init__(self, year_footer: int = 2026):
        self.year_footer = year_footer
        self.secciones = [
            Seccion("boletin", "Boletín Oficial"),
            Seccion("diputados", "Diputados"),
            Seccion("senado", "Senado"),
        ]

    # ==========================
    # HEADER (GMAIL SAFE)
    # ==========================
    def header(self) -> str:
        return f"""
<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
</head>
<body style="margin:0;padding:0;background:#ffffff;font-family:Arial,Helvetica,sans-serif;color:#263238;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#ffffff;padding:20px 0;">
<tr>
<td align="center">

<table width="780" cellpadding="0" cellspacing="0" style="max-width:780px;width:100%;">
<tr>
<td style="padding:0 18px;">

<!-- HEADER -->
<table width="100%" cellpadding="0" cellspacing="0">
<tr>
<td style="background:#06263d;padding:20px;border-radius:8px 8px 0 0;text-align:center;">
<img src="{self.LOGO_CID}" width="120" style="display:block;margin:0 auto 10px auto;border:0;">
<div style="font-size:20px;color:#ffffff;font-weight:600;">
Reporte Regulatorio Diario
</div>
<div style="font-size:11px;color:#ffffff;text-transform:uppercase;letter-spacing:1px;margin-top:6px;">
Sistema de Monitoreo de Asuntos Públicos
</div>
</td>
</tr>
<tr>
<td style="background:#ffffff;border:1px solid #e7eef7;border-top:none;padding:20px;">
<a name="top"></a>
"""

    # ==========================
    # FOOTER
    # ==========================
    def footer(self) -> str:
        return f"""
</td>
</tr>
</table>

<!-- FOOTER -->
<table width="100%" cellpadding="0" cellspacing="0" style="margin-top:20px;">
<tr>
<td style="text-align:center;font-size:12px;color:#58606a;padding:20px 0;border-top:1px solid #e7eef7;">
© {self.year_footer} BBVA Argentina • Reporte generado por Gemini
</td>
</tr>
</table>

</td>
</tr>
</table>

</td>
</tr>
</table>
</body>
</html>
"""

    # ==========================
    # TOC
    # ==========================
    def _toc(self, counts: Dict[str, int]) -> str:
        html = """
<div style="background:#f7f9fb;border:1px solid #e7eef7;border-radius:8px;padding:12px;margin-bottom:24px;">
<div style="font-weight:bold;color:#06263d;font-size:13px;margin-bottom:8px;">Índice</div>
"""
        for sec in self.secciones:
            n = counts.get(sec.key, 0)
            html += f'<a href="#sec-{sec.key}" style="color:#0a4b7a;font-size:13px;font-weight:bold;text-decoration:none;margin-right:15px;">{sec.titulo} ({n})</a>'
        html += "</div>"
        return html

    # ==========================
    # COLOR POR SECCIÓN
    # ==========================
    def _section_color(self, key: str) -> str:
        if key == "boletin":
            return "#E89A52"
        if key == "diputados":
            return "#1F7A44"
        if key == "senado":
            return "#78D7E0"
        return "#06263d"

    # ==========================
    # RENDER PRINCIPAL
    # ==========================
    def render(
        self,
        data_norm: Dict[str, Dict[str, Any]],
        meta_data_por_id: Dict[str, Dict[str, str]],
        impacto_normalizer,
        categorias_normalizer,
    ) -> str:

        def contar_items(items: List[dict]) -> int:
            return sum(
                1 for it in items
                if impacto_normalizer(it) in ("ALTO", "MEDIO")
            )

        counts = {}
        for sec in self.secciones:
            bloque = data_norm.get(sec.key, {}) or {}
            items = bloque.get("items", [])
            counts[sec.key] = contar_items(items)

        html = self.header()
        html += self._toc(counts)

        secciones_visibles = [
            sec for sec in self.secciones
            if counts.get(sec.key, 0) > 0
        ]

        for idx, sec in enumerate(secciones_visibles):

            bloque = data_norm.get(sec.key, {})
            items = bloque.get("items", [])

            html += f"""
<a name="sec-{sec.key}"></a>
<div style="margin-bottom:30px;">
<div style="border-top:6px solid {self._section_color(sec.key)};margin-bottom:15px;"></div>
<div style="font-size:13px;font-weight:bold;text-transform:uppercase;margin-bottom:6px;">
{sec.titulo} • {counts[sec.key]} ítems
</div>
"""

            for p in items:
                impacto = impacto_normalizer(p)
                if impacto not in ("ALTO", "MEDIO"):
                    continue

                titulo = p.get("titulo_descriptivo", "Sin título")
                ref = p.get("referencia", "")
                just = p.get("justificacion", "")
                link = p.get("link") or "#"

                badge_color = "#b92a2f" if impacto == "ALTO" else "#d9b34a"
                badge_text_color = "#ffffff" if impacto == "ALTO" else "#12202a"

                html += f"""
<div style="border:1px solid #f0f6fb;border-radius:8px;padding:14px;margin-bottom:16px;">
<div style="margin-bottom:8px;">
<span style="background:#f1f5f9;padding:6px 9px;border-radius:6px;font-size:11px;font-weight:bold;">{ref}</span>
<span style="background:{badge_color};color:{badge_text_color};padding:6px 9px;border-radius:6px;font-size:11px;font-weight:bold;margin-left:6px;">
Impacto {impacto}
</span>
</div>
<div style="font-size:16px;font-weight:600;margin:6px 0;">
{titulo}
</div>
<div style="font-size:14px;line-height:1.6;margin-bottom:12px;">
{just}
</div>
<a href="{link}" style="color:#0a4b7a;font-weight:bold;text-decoration:none;">
Ver Texto Oficial
</a>
</div>
"""

            html += """
<div style="margin-top:10px;">
<a href="#top" style="color:#0a4b7a;font-weight:bold;text-decoration:none;font-size:13px;">
Volver al índice
</a>
</div>
</div>
"""

            if idx < len(secciones_visibles) - 1:
                html += '<div style="height:8px;background:#06263d;border-radius:6px;margin:30px 0;"></div>'

        html += self.footer()
        return html
