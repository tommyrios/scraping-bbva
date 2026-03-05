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

    def header(self) -> str:
        return f"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
</head>
<body style="margin:0;padding:0;background:#ffffff;font-family:Arial, Helvetica, sans-serif;color:#263238;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#ffffff;padding:18px 0;">
  <tr>
    <td align="center">
      <table width="780" cellpadding="0" cellspacing="0" style="max-width:780px;width:100%;">
        <tr>
          <td style="padding:0 18px;">
            <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
              <tr>
                <td style="background:#06263d;padding:18px;border-radius:8px 8px 0 0;text-align:center;">
                  <img src="{self.LOGO_CID}" alt="BBVA" width="120" style="display:block;margin:0 auto 8px auto;border:0;outline:none;text-decoration:none;">
                  <div style="font-family:Georgia, 'Times New Roman', serif;font-size:20px;color:#ffffff;font-weight:600;line-height:1.1;">
                    Reporte Regulatorio Diario
                  </div>
                  <div style="font-size:11px;color:rgba(255,255,255,0.92);text-transform:uppercase;letter-spacing:1px;margin-top:6px;font-weight:700;">
                    Sistema de Monitoreo de Asuntos Públicos
                  </div>
                </td>
              </tr>
              <tr>
                <td style="background:#ffffff;border:1px solid #e7eef7;border-top:none;padding:18px;">
                  <a name="top"></a>
"""

    def footer(self) -> str:
        return f"""
                </td>
              </tr>
            </table>
          </td>
        </tr>
      </table>
      <table width="780" cellpadding="0" cellspacing="0" style="max-width:780px;width:100%;margin-top:14px;">
        <tr>
          <td style="padding:0 18px;">
            <table width="100%" cellpadding="0" cellspacing="0">
              <tr>
                <td style="padding:16px 0 28px 0;text-align:center;font-size:12px;color:#58606a;border-top:1px solid #e7eef7;">
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

    def _toc(self, counts: Dict[str, int]) -> str:
        html = (
            '<div style="background:#f7f9fb;border:1px solid #e7eef7;border-radius:8px;padding:12px;margin-bottom:22px;">'
            '<div style="font-weight:700;color:#06263d;font-size:13px;margin-bottom:8px;">Índice</div>'
        )
        for sec in self.secciones:
            n = counts.get(sec.key, 0)
            html += (
                f'<a href="#sec-{sec.key}" style="color:#0a4b7a;font-size:13px;font-weight:700;text-decoration:none;margin-right:14px;">'
                f'{sec.titulo} ({n})</a>'
            )
        html += "</div>"
        return html

    def _section_color(self, key: str) -> str:
        if key == "boletin":
            return "#E89A52"
        if key == "diputados":
            return "#1F7A44"
        if key == "senado":
            return "#78D7E0"
        return "#06263d"

    def render(
        self,
        data_norm: Dict[str, Dict[str, Any]],
        meta_data_por_id: Dict[str, Dict[str, str]],
        impacto_normalizer,
        categorias_normalizer,
    ) -> str:
        def contar_items(items: List[dict]) -> int:
            cnt = 0
            for it in items:
                try:
                    lvl = impacto_normalizer(it)
                except Exception:
                    lvl = "BAJO"
                if lvl in ("ALTO", "MEDIO"):
                    cnt += 1
            return cnt

        counts: Dict[str, int] = {}
        for sec in self.secciones:
            bloque = data_norm.get(sec.key, {}) or {}
            items = bloque.get("items", []) if isinstance(bloque, dict) else []
            counts[sec.key] = contar_items(items)

        html = self.header()
        html += self._toc(counts)

        secciones_visibles = [sec for sec in self.secciones if counts.get(sec.key, 0) > 0]

        if not secciones_visibles:
            html += (
                '<div style="text-align:center;padding:36px 0;color:#58606a;">'
                '<div style="font-size:16px;color:#06263d;font-weight:700;margin-bottom:6px;">Sin Novedades de Impacto</div>'
                '<div>No hubo ítems con impacto Alto o Medio.</div>'
                '</div>'
            )
            html += self.footer()
            return html

        orden_impacto = {"ALTO": 0, "MEDIO": 1}

        for idx, sec in enumerate(secciones_visibles):
            bloque = data_norm.get(sec.key, {}) or {}
            items = bloque.get("items", []) if isinstance(bloque, dict) else []

            color_bar = self._section_color(sec.key)
            html += (
                f'<a name="sec-{sec.key}"></a>'
                f'<div style="margin-bottom:28px;">'
                f'<div style="border-top:6px solid {color_bar};margin-bottom:12px;border-radius:4px;"></div>'
                f'<div style="font-size:13px;font-weight:700;text-transform:uppercase;margin-bottom:8px;color:#081122;">'
                f'{sec.titulo} • {counts.get(sec.key, 0)} ítems</div>'
            )

            resumen = ""
            if isinstance(bloque, dict):
                resumen = (bloque.get("resumen") or "") or ""
            if resumen:
                html += (
                    f'<div style="background:#f3fbff;border-left:4px solid #0a4b7a;'
                    f'padding:10px;border-radius:6px;margin-bottom:12px;color:#1f3642;font-size:14px;">'
                    f'{resumen}</div>'
                )

            items_email: List[dict] = []
            for p in items:
                try:
                    imp = impacto_normalizer(p)
                except Exception:
                    imp = "BAJO"
                if imp in ("ALTO", "MEDIO"):
                    items_email.append(p)

            items_ordenados = sorted(
                items_email,
                key=lambda x: orden_impacto.get(impacto_normalizer(x), 99),
            )

            for p in items_ordenados:
                try:
                    impacto = impacto_normalizer(p)
                except Exception:
                    impacto = "BAJO"

                id_ref = str(p.get("id_interno", "")).strip()
                meta = meta_data_por_id.get(id_ref, {}) if id_ref else {}

                titulo = p.get("titulo_descriptivo") or meta.get("titulo") or "Sin título"
                ref = p.get("referencia") or meta.get("referencia") or ""
                just = (p.get("justificacion") or "").strip()
                link = meta.get("link") or p.get("link") or "#"
                fuente = meta.get("fuente") or p.get("source") or ""
                fecha = p.get("fecha") or p.get("date") or ""

                autor = (meta.get("autor") or p.get("autor") or p.get("autor_item") or "").strip()
                if autor and autor.upper() in ("S/D", "SD", "N/A", "NA"):
                    autor = ""

                if impacto == "ALTO":
                    badge_bg = "#b92a2f"
                    badge_color = "#ffffff"
                else:
                    badge_bg = "#d9b34a"
                    badge_color = "#12202a"

                categorias = categorias_normalizer(p) if categorias_normalizer else []
                cat_html = ""
                for c in categorias:
                    cat_html += (
                        f'<span style="display:inline-block;background:#f1f5f9;color:#475569;'
                        f'padding:6px 9px;border-radius:6px;font-size:11px;font-weight:700;'
                        f'margin-right:6px;margin-bottom:6px;">{c}</span>'
                    )

                html += (
                    '<div style="border:1px solid #f0f6fb;border-radius:8px;'
                    'padding:12px;margin-bottom:14px;background:#ffffff;">'
                    '<div style="margin-bottom:8px;">'
                    f'<span style="display:inline-block;background:#f1f5f9;color:#475569;'
                    f'padding:6px 9px;border-radius:6px;font-size:11px;font-weight:700;margin-right:6px;">{ref}</span>'
                    f'<span style="display:inline-block;background:{badge_bg};color:{badge_color};'
                    f'padding:6px 9px;border-radius:6px;font-size:11px;font-weight:700;margin-right:6px;">Impacto {impacto}</span>'
                    f'{cat_html}'
                    '</div>'
                    f'<div style="font-family:Georgia, \'Times New Roman\', serif;font-size:16px;color:#071226;margin:6px 0;font-weight:600;">{titulo}</div>'
                )

                if fuente or fecha:
                    html += (
                        f'<div style="font-size:13px;color:#58606a;margin-bottom:8px;">'
                        f'{fuente}{" • " if fuente and fecha else ""}{fecha}'
                        f'</div>'
                    )

                if autor:
                    html += (
                        f'<div style="font-size:13px;color:#58606a;margin-bottom:8px;">'
                        f'Autor: <b>{autor}</b>'
                        f'</div>'
                    )

                if just:
                    html += f'<div style="font-size:14px;line-height:1.6;margin-bottom:10px;color:#24333a;">{just}</div>'

                html += f'<a href="{link}" style="color:#0a4b7a;font-weight:700;text-decoration:none;font-size:13px;">Ver Texto Oficial</a>'
                html += '</div>'

            html += (
                '<div style="margin-top:10px;">'
                '<a href="#top" style="color:#0a4b7a;font-weight:700;text-decoration:none;font-size:13px;">'
                'Volver al índice</a></div>'
            )
            html += '</div>'

            if idx < len(secciones_visibles) - 1:
                html += '<div style="height:8px;background:#06263d;border-radius:6px;margin:28px 0;"></div>'

        html += self.footer()
        return html
