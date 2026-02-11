from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Any


@dataclass
class SectionConfig:
    key: str
    title: str
    emoji: str
    bg: str
    border: str
    kicker: str
    sub: str
    link_color: str


class ReporteUI:
    """
    Responsabilidad: Renderizar el HTML del reporte (email-friendly).
    No sabe nada de Gemini. Solo recibe data ya normalizada.
    """

    def __init__(self, logo_cid: str = "cid:bbva_logo"):
        self.logo_cid = logo_cid

        self.sections: List[SectionConfig] = [
            SectionConfig(
                key="boletin",
                title="Boletín Oficial",
                emoji="🏛️",
                bg="#E8F0FE",
                border="#1A73E8",
                kicker="#174EA6",
                sub="Normas y resoluciones publicadas en el Boletín Oficial.",
                link_color="#1A73E8",
            ),
            SectionConfig(
                key="diputados",
                title="Diputados",
                emoji="🏛️",
                bg="#E6F4EA",
                border="#1E8E3E",
                kicker="#137333",
                sub="Actividad parlamentaria en Cámara de Diputados.",
                link_color="#1E8E3E",
            ),
            SectionConfig(
                key="senado",
                title="Senado",
                emoji="🏛️",
                bg="#FEF7E0",
                border="#F9AB00",
                kicker="#B06000",
                sub="Actividad parlamentaria en Cámara de Senadores.",
                link_color="#F9AB00",
            ),
        ]

    def render(
        self,
        data_norm: Dict[str, Dict[str, Any]],
        meta_data_por_id: Dict[str, Dict[str, str]],
        normalizar_impacto_fn,
        normalizar_categorias_fn,
        email_filtra_niveles: Tuple[str, ...] = ("ALTO", "MEDIO"),
    ) -> str:
        """
        data_norm: { 'boletin': {resumen:str, items:[...]}, ... }
        meta_data_por_id: {id_interno: {titulo, link, referencia, autor}}
        normalizar_impacto_fn / normalizar_categorias_fn: funciones del Analista
        """

        html = self._header()

        conteos = self._contar_items_por_seccion(
            data_norm, normalizar_impacto_fn, email_filtra_niveles
        )

        html += self._toc(conteos)

        hay_contenido = False

        for sec in self.sections:
            bloque = data_norm.get(sec.key, {}) if isinstance(data_norm, dict) else {}
            items = bloque.get("items", []) if isinstance(bloque, dict) else []
            resumen = bloque.get("resumen", "") if isinstance(bloque, dict) else ""

            items_email = [p for p in items if normalizar_impacto_fn(p) in email_filtra_niveles]
            if not items_email:
                continue

            hay_contenido = True

            html += self._section_separator()
            html += f'<a id="sec-{sec.key}"></a>'
            html += self._section_banner(sec, len(items_email))

            if resumen:
                html += f'<div class="resumen-block">{self._esc(resumen)}</div>'

            orden = {"ALTO": 1, "MEDIO": 2, "BAJO": 3}
            items_ordenados = sorted(items_email, key=lambda x: orden.get(normalizar_impacto_fn(x), 99))

            for p in items_ordenados:
                html += self._render_item(
                    p=p,
                    meta_data_por_id=meta_data_por_id,
                    normalizar_impacto_fn=normalizar_impacto_fn,
                    normalizar_categorias_fn=normalizar_categorias_fn,
                )

        if not hay_contenido:
            html += """
            <div class="empty-state">
                <span class="empty-icon">✅</span>
                <h3>Sin Novedades de Impacto</h3>
                <p>No hubo items con impacto <b>Alto</b> o <b>Medio</b>.</p>
            </div>
            """

        html += self._footer()
        return html

    def _header(self) -> str:
        return f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <style>
    body {{
      font-family: 'Segoe UI', 'Roboto', Helvetica, Arial, sans-serif;
      color: #333;
      line-height: 1.6;
      background-color: #ffffff;
      margin: 0;
      padding: 0;
    }}
    .container {{
      width: 100%;
      max-width: 100%;
      margin: 0;
      background: #fff;
    }}
    .header {{
      background-color: #072146;
      color: white;
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
      font-weight: 500;
      letter-spacing: 0.5px;
      color: #ffffff;
    }}
    .header h2 {{
      margin: 8px 0 0;
      font-size: 13px;
      opacity: 0.8;
      font-weight: 400;
      text-transform: uppercase;
      letter-spacing: 2px;
      color: #a4c4e0;
    }}
    .content {{
      padding: 28px 5% 40px 5%;
      background-color: #ffffff;
    }}

    /* TOC */
    .toc {{
      background: #f4f8fb;
      border: 1px solid #d7e9f7;
      border-radius: 10px;
      padding: 14px 16px;
      margin: 0 0 22px 0;
      font-size: 13px;
    }}
    .toc-title {{
      font-weight: 800;
      margin-bottom: 8px;
      color: #072146;
    }}
    .toc a {{
      text-decoration: none;
      font-weight: 700;
      margin-right: 14px;
      display: inline-block;
      margin-bottom: 6px;
    }}
    .toc-muted {{
      color: #666;
      font-weight: 400;
      margin-top: 6px;
    }}

    /* Separador fuerte */
    .section-separator {{
      border-top: 4px solid #072146;
      margin: 26px 0 14px 0;
    }}

    /* Banner de sección */
    .section-banner {{
      border-radius: 12px;
      padding: 14px 16px;
      margin-top: 10px;
      margin-bottom: 18px;
      border-left: 7px solid #072146;
    }}
    .section-kicker {{
      font-size: 12px;
      letter-spacing: 1.4px;
      font-weight: 800;
      text-transform: uppercase;
      margin: 0;
    }}
    .section-sub {{
      font-size: 12px;
      margin-top: 6px;
      color: #3c4043;
    }}

    /* Resumen */
    .resumen-block {{
      background-color: #f4f8fb;
      border-left: 5px solid #1973b8;
      padding: 18px;
      margin-bottom: 26px;
      font-style: italic;
      color: #444;
      font-size: 15px;
      border-radius: 8px;
    }}

    /* Items */
    .item {{
      margin-bottom: 30px;
      padding-bottom: 22px;
      border-bottom: 1px solid #eeeeee;
    }}
    .item:last-child {{ border-bottom: none; }}
    .badges-row {{ margin-bottom: 10px; }}
    .badge {{
      padding: 6px 12px;
      border-radius: 4px;
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      display: inline-block;
      vertical-align: middle;
      margin-right: 8px;
      margin-bottom: 6px;
    }}
    .bg-alto {{ background-color: #da3851; color: white; }}
    .bg-medio {{ background-color: #f8cd51; color: #121212; }}
    .bg-bajo {{ background-color: #d7e9f7; color: #072146; border: 1px solid #b9d6ef; }}
    .bg-ref {{ background-color: #f2f2f2; color: #555; border: 1px solid #ddd; }}

    .item-title {{
      font-size: 18px;
      font-weight: 700;
      color: #121212;
      margin: 0 0 8px 0;
      line-height: 1.4;
    }}
    .autor {{
      font-size: 13px;
      color: #666;
      margin-top: -2px;
      margin-bottom: 10px;
    }}
    .justificacion {{
      font-size: 15px;
      color: #444;
      margin-bottom: 14px;
      text-align: left;
      line-height: 1.6;
    }}
    .btn-link {{
      display: inline-block;
      font-size: 12px;
      color: #004481;
      text-decoration: none;
      font-weight: 700;
      border: 2px solid #004481;
      padding: 10px 20px;
      border-radius: 6px;
      text-transform: uppercase;
    }}
    .empty-state {{ text-align: center; padding: 40px 0; color: #666; }}
    .empty-icon {{ font-size: 40px; margin-bottom: 15px; display: block; opacity: 0.5; }}

    .footer {{
      background-color: #f9f9f9;
      padding: 30px 5%;
      text-align: center;
      font-size: 12px;
      color: #999;
      border-top: 1px solid #eaeaea;
    }}
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="logo-row">
        <img src="{self.logo_cid}" alt="BBVA" class="logo-img">
      </div>
      <div class="title-row">
        <h1>Reporte Regulatorio Diario</h1>
        <h2>Sistema de Monitoreo de Asuntos Públicos</h2>
      </div>
    </div>
    <div class="content">
"""

    def _footer(self) -> str:
        return """
    </div>
    <div class="footer">
      &copy; 2026 BBVA Argentina • Generado por Inteligencia Artificial (Gemini)
    </div>
  </div>
</body>
</html>
"""

    def _toc(self, conteos: Dict[str, int]) -> str:
        parts = ['<div class="toc">', '<div class="toc-title">Índice</div>']
        any_link = False

        for sec in self.sections:
            c = conteos.get(sec.key, 0)
            if c <= 0:
                continue
            any_link = True
            parts.append(
                f'<a href="#sec-{sec.key}" style="color:{sec.link_color};">• {sec.title} ({c})</a>'
            )

        if not any_link:
            parts.append('<div class="toc-muted">Sin secciones con impacto Alto/Medio en esta ejecución.</div>')
        else:
            parts.append('<div class="toc-muted">Tip: tocá una sección para saltar directo.</div>')

        parts.append("</div>")
        return "".join(parts)

    def _section_separator(self) -> str:
        return '<div class="section-separator"></div>'

    def _section_banner(self, sec: SectionConfig, count: int) -> str:
        return f"""
<div class="section-banner" style="background:{sec.bg}; border-left-color:{sec.border};">
  <div class="section-kicker" style="color:{sec.kicker};">{sec.emoji} {sec.title} • {count} ítems</div>
  <div class="section-sub">{self._esc(sec.sub)}</div>
</div>
"""

    def _render_item(
        self,
        p: Dict[str, Any],
        meta_data_por_id: Dict[str, Dict[str, str]],
        normalizar_impacto_fn,
        normalizar_categorias_fn,
    ) -> str:
        id_ref = str(p.get("id_interno", "")).strip()
        meta = meta_data_por_id.get(id_ref, {})

        titulo_mostrar = p.get("titulo_descriptivo") or meta.get("titulo") or "Sin título"
        ref = p.get("referencia") or meta.get("referencia") or ""
        link_web = meta.get("link") or "#"
        justificacion = p.get("justificacion", "") or ""

        impacto = normalizar_impacto_fn(p)
        categorias = normalizar_categorias_fn(p)

        autor_item = (meta.get("autor") or "").strip()
        autor_html = ""
        if autor_item and autor_item.upper() != "S/D":
            autor_html = f'<div class="autor">Autor: <b>{self._esc(autor_item)}</b></div>'

        if impacto == "ALTO":
            clase_badge = "bg-alto"
        elif impacto == "MEDIO":
            clase_badge = "bg-medio"
        else:
            clase_badge = "bg-bajo"

        cat_badges = "".join([f'<span class="badge bg-ref">{self._esc(c)}</span>' for c in categorias])

        return f"""
<div class="item">
  <div class="badges-row">
    <span class="badge bg-ref">{self._esc(ref)}</span>
    <span class="badge {clase_badge}">IMPACTO {self._esc(impacto)}</span>
    {cat_badges}
  </div>
  <div class="item-title">{self._esc(titulo_mostrar)}</div>
  {autor_html}
  <div class="justificacion">{self._esc(justificacion)}</div>
  <a href="{self._esc_attr(link_web)}" target="_blank" class="btn-link">Ver Texto Oficial &rarr;</a>
</div>
"""

    def _contar_items_por_seccion(self, data_norm, normalizar_impacto_fn, niveles):
        conteos = {}
        for sec in self.sections:
            bloque = data_norm.get(sec.key, {}) if isinstance(data_norm, dict) else {}
            items = bloque.get("items", []) if isinstance(bloque, dict) else []
            items_email = [p for p in items if normalizar_impacto_fn(p) in niveles]
            conteos[sec.key] = len(items_email)
        return conteos

    def _esc(self, s: str) -> str:
        if s is None:
            return ""
        s = str(s)
        return (
            s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;")
        )

    def _esc_attr(self, s: str) -> str:
        return self._esc(s).replace("\n", "").replace("\r", "")
