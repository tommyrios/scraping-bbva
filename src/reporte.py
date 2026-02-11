from dataclasses import dataclass
from typing import Dict, List, Tuple, Any


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
        return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  body {{
    font-family: 'Segoe UI', Arial, Helvetica, sans-serif;
    color: #1f2937;
    line-height: 1.6;
    background-color: #ffffff;
    margin: 0;
    padding: 0;
  }}
  .container {{
    width: 100%;
    max-width: 100%;
    margin: 0;
    background: #ffffff;
  }}
  .header {{
    background-color: #072146;
    color: #ffffff;
    padding: 30px 5%;
  }}
  .logo-row {{
    text-align: left;
    margin-bottom: 18px;
    width: 100%;
  }}
  .logo-img {{
    height: 44px;
    width: auto;
    display: block;
    border: 0;
    outline: none;
    text-decoration: none;
  }}
  .title-row {{
    text-align: center;
    width: 100%;
  }}
  .header h1 {{
    margin: 0;
    font-size: 26px;
    font-weight: 600;
    letter-spacing: 0.2px;
    color: #ffffff;
  }}
  .header h2 {{
    margin: 8px 0 0;
    font-size: 12px;
    opacity: 0.85;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 2px;
    color: #85C8FF;
  }}
  .content {{
    padding: 34px 5% 40px 5%;
    background-color: #ffffff;
  }}

  .toc {{
    background: #F7F8FA;
    border: 1px solid #E6E8EE;
    border-radius: 10px;
    padding: 14px 16px;
    margin: 0 0 18px 0;
  }}
  .toc-title {{
    font-weight: 700;
    color: #072146;
    font-size: 13px;
    margin: 0 0 10px 0;
  }}
  .toc a {{
    text-decoration: none;
    font-weight: 700;
    font-size: 13px;
    margin-right: 14px;
    display: inline-block;
    margin-bottom: 6px;
    color: #004481;
  }}
  .toc-muted {{
    color: #5b6472;
    font-size: 12px;
    margin-top: 8px;
  }}

  .section-separator {{
    border-top: 6px solid #072146;
    margin: 44px 0 18px 0;
    padding-top: 10px;
  }}
  .section-banner {{
    border-radius: 12px;
    padding: 18px 18px;
    margin: 0 0 22px 0;
    border-left: 10px solid #072146;
    background: #F4F8FB;
  }}
  .section-kicker {{
    margin: 0;
    font-size: 12px;
    font-weight: 800;
    text-transform: uppercase;
    letter-spacing: 1.6px;
    color: #072146;
  }}
  .section-sub {{
    margin-top: 6px;
    font-size: 12px;
    color: #495163;
  }}

  .banner-boletin {{
    background: #F4F8FB;
    border-left-color: #001391;
  }}
  .banner-boletin .section-kicker {{
    color: #001391;
  }}

  .banner-diputados {{
    background: #F4F8FB;
    border-left-color: #004481;
  }}
  .banner-diputados .section-kicker {{
    color: #004481;
  }}

  .banner-senado {{
    background: #F4F8FB;
    border-left-color: #060E46;
  }}
  .banner-senado .section-kicker {{
    color: #060E46;
  }}

  .resumen-block {{
    background-color: #F4F8FB;
    border-left: 5px solid #004481;
    padding: 18px 18px;
    margin: 0 0 26px 0;
    font-style: italic;
    color: #334155;
    font-size: 14px;
    border-radius: 8px;
  }}

  .item {{
    margin-bottom: 34px;
    padding-bottom: 24px;
    border-bottom: 1px solid #E6E8EE;
  }}
  .item:last-child {{ border-bottom: none; }}

  .badges-row {{ margin-bottom: 10px; }}

  .badge {{
    padding: 6px 10px;
    border-radius: 6px;
    font-size: 11px;
    font-weight: 800;
    text-transform: uppercase;
    display: inline-block;
    vertical-align: middle;
    margin-right: 8px;
    margin-bottom: 6px;
  }}
  .bg-ref {{
    background-color: #F2F3F6;
    color: #4B5563;
    border: 1px solid #E6E8EE;
  }}
  .bg-alto {{
    background-color: #7A1E2B;
    color: #ffffff;
  }}
  .bg-medio {{
    background-color: #C79A2B;
    color: #111827;
  }}
  .bg-bajo {{
    background-color: #E8F0FE;
    color: #072146;
    border: 1px solid #D7E9F7;
  }}

  .item-title {{
    font-size: 18px;
    font-weight: 800;
    color: #111827;
    margin: 0 0 8px 0;
    line-height: 1.35;
  }}
  .autor {{
    font-size: 13px;
    color: #6B7280;
    margin-top: -2px;
    margin-bottom: 10px;
  }}
  .justificacion {{
    font-size: 14px;
    color: #374151;
    margin-bottom: 14px;
    text-align: left;
    line-height: 1.65;
  }}

  .btn-link {{
    display: inline-block;
    font-size: 12px;
    color: #004481;
    text-decoration: none;
    font-weight: 800;
    border: 2px solid #004481;
    padding: 10px 16px;
    border-radius: 8px;
    text-transform: uppercase;
  }}

  .back-to-index {{
    margin: 16px 0 0 0;
    text-align: left;
  }}
  .back-to-index a {{
    font-size: 12px;
    font-weight: 800;
    color: #004481;
    text-decoration: none;
  }}

  .empty-state {{
    text-align: center;
    padding: 40px 0;
    color: #6B7280;
  }}

  .footer {{
    background-color: #F7F8FA;
    padding: 26px 5%;
    text-align: center;
    font-size: 12px;
    color: #6B7280;
    border-top: 1px solid #E6E8EE;
  }}
</style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="logo-row">
        <img src="{self.LOGO_CID}" alt="" class="logo-img" width="140" height="44" style="display:block;border:0;outline:none;text-decoration:none;">
      </div>
      <div class="title-row">
        <h1>Reporte Regulatorio Diario</h1>
        <h2>Sistema de Monitoreo de Asuntos Públicos</h2>
      </div>
    </div>
    <div class="content">
"""

    def footer(self) -> str:
        return f"""
    </div>
    <div class="footer">
      &copy; {self.year_footer} BBVA Argentina • Reporte generado por Gemini
    </div>
  </div>
</body>
</html>
"""

    def empty(self, mensaje: str) -> str:
        return self.header() + f"""
  <div class="empty-state">
    <h3 style="margin:0 0 8px 0;color:#072146;">Sin Novedades</h3>
    <p style="margin:0;">{mensaje}</p>
  </div>
""" + self.footer()

    def _banner_class(self, key: str) -> str:
        if key == "boletin":
            return "section-banner banner-boletin"
        if key == "diputados":
            return "section-banner banner-diputados"
        if key == "senado":
            return "section-banner banner-senado"
        return "section-banner"

    def _toc(self, counts: Dict[str, int]) -> str:
        parts = ['<a name="top" id="top"></a>', '<div class="toc">', '<div class="toc-title">Índice</div>']
        for sec in self.secciones:
            n = int(counts.get(sec.key, 0) or 0)
            if n <= 0:
                continue
            parts.append(f'<a href="#sec-{sec.key}">{sec.titulo} ({n})</a>')
        parts.append('</div>')
        return "\n".join(parts)

    def _separator(self) -> str:
        return '<div class="section-separator"></div>'

    def _section_header(self, key: str, titulo: str, count: int, subtitle: str) -> str:
        banner_cls = self._banner_class(key)
        return f"""
{self._separator()}
<a name="sec-{key}" id="sec-{key}"></a>
<div class="{banner_cls}">
  <div class="section-kicker">{titulo} • {count} ítems</div>
  <div class="section-sub">{subtitle}</div>
</div>
"""

    def render(
        self,
        data_norm: Dict[str, Dict[str, Any]],
        meta_data_por_id: Dict[str, Dict[str, str]],
        impacto_normalizer,
        categorias_normalizer,
    ) -> str:
        def contar_items_email(items: List[dict]) -> int:
            c = 0
            for it in items:
                lvl = impacto_normalizer(it)
                if lvl in ("ALTO", "MEDIO"):
                    c += 1
            return c

        counts = {}
        for sec in self.secciones:
            bloque = data_norm.get(sec.key, {}) or {}
            items = bloque.get("items", []) if isinstance(bloque, dict) else []
            counts[sec.key] = contar_items_email(items)

        html = self.header()
        html += self._toc(counts)

        hay_contenido = any((counts.get(sec.key, 0) or 0) > 0 for sec in self.secciones)

        if not hay_contenido:
            html += """
  <div class="empty-state">
    <h3 style="margin:0 0 8px 0;color:#072146;">Sin Novedades de Impacto</h3>
    <p style="margin:0;">No hubo ítems con impacto Alto o Medio.</p>
  </div>
"""
            html += self.footer()
            return html

        subt = {
            "boletin": "Normas y resoluciones publicadas en el Boletín Oficial.",
            "diputados": "Actividad parlamentaria en Cámara de Diputados.",
            "senado": "Actividad parlamentaria en Cámara de Senadores.",
        }

        orden = {"ALTO": 1, "MEDIO": 2, "BAJO": 3}

        for sec in self.secciones:
            bloque = data_norm.get(sec.key, {}) or {}
            items = bloque.get("items", []) if isinstance(bloque, dict) else []
            resumen = (bloque.get("resumen", "") if isinstance(bloque, dict) else "") or ""

            items_email = [p for p in items if impacto_normalizer(p) in ("ALTO", "MEDIO")]
            if not items_email:
                continue

            html += self._section_header(sec.key, sec.titulo, len(items_email), subt.get(sec.key, ""))

            if resumen:
                html += f'<div class="resumen-block">{resumen}</div>'

            items_ordenados = sorted(items_email, key=lambda x: orden.get(impacto_normalizer(x), 99))

            for p in items_ordenados:
                id_ref = str(p.get("id_interno", "")).strip()
                meta = meta_data_por_id.get(id_ref, {}) if id_ref else {}

                titulo_mostrar = p.get("titulo_descriptivo") or meta.get("titulo") or "Sin título"
                ref = p.get("referencia") or meta.get("referencia") or ""
                link_web = meta.get("link") or "#"
                justificacion = (p.get("justificacion", "") or "").strip()

                impacto = impacto_normalizer(p)
                categorias = categorias_normalizer(p)

                autor_item = (meta.get("autor") or "").strip()
                autor_html = f'<div class="autor">Autor: <b>{autor_item}</b></div>' if (autor_item and autor_item.upper() != "S/D") else ""

                if impacto == "ALTO":
                    clase_badge = "bg-alto"
                elif impacto == "MEDIO":
                    clase_badge = "bg-medio"
                else:
                    clase_badge = "bg-bajo"

                cat_badges = "".join([f'<span class="badge bg-ref">{c}</span>' for c in categorias])

                html += f"""
<div class="item">
  <div class="badges-row">
    <span class="badge bg-ref">{ref}</span>
    <span class="badge {clase_badge}">Impacto {impacto}</span>
    {cat_badges}
  </div>
  <div class="item-title">{titulo_mostrar}</div>
  {autor_html}
  <div class="justificacion">{justificacion}</div>
  <a href="{link_web}" target="_blank" class="btn-link">Ver Texto Oficial</a>
</div>
"""

            html += f"""
<div class="back-to-index">
  <a href="#top">Volver al índice</a>
</div>
"""

        html += self.footer()
        return html
