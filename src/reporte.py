from dataclasses import dataclass
from typing import Dict, List, Any


@dataclass(frozen=True)
class Seccion:
    key: str
    titulo: str


class ReporteUI:
    LOGO_CID = "cid:bbva_logo"

    COLOR_NAVY = "#06263d"
    COLOR_LINK = "#0a4b7a"
    COLOR_ALTO = "#b92a2f"
    COLOR_MEDIO = "#d9b34a"
    COLOR_DIPUTADOS = "#1F7A44"
    COLOR_MUTED = "#58606a"
    COLOR_BG_SOFT = "#f7f9fb"
    COLOR_BORDER = "#e7eef7"

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
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<link href="https://fonts.googleapis.com/css2?family=Lato:wght@300;400;700&family=Source+Serif+4:wght@600;700&display=swap" rel="stylesheet">
<style>
  :root{{
    --navy: {self.COLOR_NAVY};
    --link: {self.COLOR_LINK};
    --alto: {self.COLOR_ALTO};
    --medio: {self.COLOR_MEDIO};
    --dip-green: {self.COLOR_DIPUTADOS};
    --muted: {self.COLOR_MUTED};
    --bg-soft: {self.COLOR_BG_SOFT};
    --border: {self.COLOR_BORDER};
    --max-width: 780px;
  }}

  html,body{{margin:0;padding:0;background:#fff;color:#263238;font-family:'Lato',Arial,Helvetica,sans-serif;}}
  .wrap{{max-width:var(--max-width);margin:18px auto;padding:0 18px;}}

  .header{{background:var(--navy);color:#fff;padding:20px 28px;border-radius:8px 8px 0 0;}}
  .title{{text-align:center;margin-top:8px}}
  .title h1{{margin:0;font-family:'Source Serif 4',serif;font-size:22px;font-weight:600;}}
  .title h2{{margin:6px 0 0;font-size:11px;text-transform:uppercase;letter-spacing:1.2px;font-weight:700;opacity:.95}}

  .card{{background:#fff;border:1px solid var(--border);border-top:none;padding:22px;border-radius:0 0 8px 8px;}}

  .toc{{background:var(--bg-soft);border:1px solid var(--border);border-radius:10px;padding:10px 12px;margin-bottom:24px;}}
  .toc-title{{font-weight:700;color:var(--navy);font-size:13px;margin-bottom:6px}}
  .toc a{{color:var(--link);font-weight:700;font-size:13px;text-decoration:none;margin-right:12px}}

  .section-wrap{{margin-bottom:38px;}}
  .section-divider{{height:8px;background:var(--navy);border-radius:6px;margin:30px 0;}}

  .section-banner{{border:1px solid var(--border);border-radius:10px;padding:12px 14px;margin-bottom:14px}}
  .section-bar{{height:6px;border-radius:999px;margin-bottom:8px}}
  .section-kicker{{font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:1px}}
  .section-sub{{font-size:12px;color:var(--muted);margin-top:6px}}

  .item{{border:1px solid #f0f6fb;border-radius:8px;padding:14px 10px;margin-bottom:16px}}
  .badges{{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px}}
  .badge{{font-size:11px;padding:6px 9px;border-radius:8px;font-weight:700;text-transform:uppercase}}
  .badge.ref{{background:#f1f5f9;color:#475569;border:1px solid var(--border)}}
  .badge.alto{{background:var(--alto);color:#fff}}
  .badge.medio{{background:var(--medio);color:#12202a}}

  .item-title{{font-family:'Source Serif 4',serif;font-size:16px;font-weight:600;margin:6px 0}}
  .meta{{font-size:13px;color:var(--muted);margin-bottom:8px}}
  .justificacion{{font-size:14px;line-height:1.6;margin-bottom:12px}}
  .cta{{display:inline-block;padding:9px 13px;border-radius:8px;border:1px solid var(--link);color:var(--link);font-weight:700;text-decoration:none;font-size:13px}}

  .back{{margin-top:12px}}
  .back a{{color:var(--link);font-weight:700;text-decoration:none;font-size:13px}}

  .footer{{text-align:center;font-size:12px;color:var(--muted);padding:20px 0;border-top:1px solid var(--border)}}

  @media (max-width:560px){{
    .wrap{{padding:0 12px}}
    .header{{padding:16px}}
    .title h1{{font-size:20px}}
    .item-title{{font-size:15px}}
  }}
</style>
</head>
<body>
<div class="wrap">
  <div class="header">
    <div class="title">
      <h1>Reporte Regulatorio Diario</h1>
      <h2>Sistema de Monitoreo de Asuntos Públicos</h2>
    </div>
  </div>
  <div class="card">
    <a id="top"></a>
"""

    def footer(self) -> str:
        return f"""
  </div> <!-- card -->
  <div class="footer">© {self.year_footer} BBVA Argentina • Reporte generado por Gemini</div>
</div>
</body>
</html>
"""

    def empty(self, mensaje: str) -> str:
        return self.header() + f"""
  <div style="text-align:center;padding:40px 0;color:{self.COLOR_MUTED}">
    <h3 style="margin:0 0 8px 0;color:{self.COLOR_NAVY};font-weight:700;">Sin Novedades</h3>
    <p style="margin:0;">{mensaje}</p>
  </div>
""" + self.footer()

    def _section_header(self, key: str, titulo: str, count: int, subtitle: str, color_bar: str) -> str:
        return f"""
    <div class="section-banner">
      <div class="section-bar" style="background:{color_bar}"></div>
      <div class="section-kicker">{titulo} • {count} ítems</div>
      <div class="section-sub">{subtitle}</div>
    </div>
"""

    def _section_divider(self) -> str:
        return '<div class="section-divider"></div>'

    def _toc(self, counts: Dict[str, int]) -> str:
        parts = ['<div class="toc">', '<div class="toc-title">Índice</div>']
        for sec in self.secciones:
            n = int(counts.get(sec.key, 0) or 0)
            parts.append(f'<a href="#sec-{sec.key}">{sec.titulo} ({n})</a>')
        parts.append('</div>')
        return "\n".join(parts)

    def _section_color(self, key: str) -> str:
        if key == "boletin":
            return "#E89A52"
        if key == "diputados":
            return self.COLOR_DIPUTADOS
        if key == "senado":
            return "#78D7E0"
        return self.COLOR_NAVY

    def render(
        self,
        data_norm: Dict[str, Dict[str, Any]],
        meta_data_por_id: Dict[str, Dict[str, str]],
        impacto_normalizer,
        categorias_normalizer,
    ) -> str:
        """
        Renderiza el HTML final. Lógica:
         - calcula qué secciones tienen items (ALTO/MEDIO)
         - renderiza solo esas secciones
         - inserta la barra separadora **solo** entre secciones (no después de la última)
         - agrega el link 'Volver al índice' al final de cada sección
        """
        def contar_items(items: List[dict]) -> int:
            c = 0
            for it in items:
                try:
                    lvl = impacto_normalizer(it)
                except Exception:
                    lvl = "BAJO"
                if lvl in ("ALTO", "MEDIO"):
                    c += 1
            return c

        counts = {}
        for sec in self.secciones:
            bloque = data_norm.get(sec.key, {}) or {}
            items = bloque.get("items", []) if isinstance(bloque, dict) else []
            counts[sec.key] = contar_items(items)

        html = self.header()
        html += self._toc(counts)

        secciones_a_mostrar: List[Seccion] = []
        for sec in self.secciones:
            bloque = data_norm.get(sec.key, {}) or {}
            items = bloque.get("items", []) if isinstance(bloque, dict) else []
            if any((impacto_normalizer(it) if impacto_normalizer else "BAJO") in ("ALTO", "MEDIO") for it in items):
                secciones_a_mostrar.append(sec)

        if not secciones_a_mostrar:
            html += """
  <div style="text-align:center;padding:40px 0;color:#6b7280">
    <h3 style="margin:0 0 8px 0;color:#06263d;font-weight:700;">Sin Novedades de Impacto</h3>
    <p style="margin:0;">No hubo ítems con impacto Alto o Medio.</p>
  </div>
"""
            html += self.footer()
            return html

        for idx, sec in enumerate(secciones_a_mostrar):
            bloque = data_norm.get(sec.key, {}) or {}
            items = bloque.get("items", []) if isinstance(bloque, dict) else []
            resumen = (bloque.get("resumen", "") if isinstance(bloque, dict) else "") or ""

            items_email = [p for p in items if (impacto_normalizer(p) if impacto_normalizer else "BAJO") in ("ALTO", "MEDIO")]
            if not items_email:
                continue

            color_bar = self._section_color(sec.key)
            html += f'\n    <a id="sec-{sec.key}"></a>\n    <div class="section-wrap">\n'
            html += self._section_header(sec.key, sec.titulo, len(items_email), resumen or "", color_bar)

            if resumen:
                html += f'    <div class="resumen">{resumen}</div>\n'

            # Render items en el orden original (o podés ordenar por impacto si querés)
            for p in items_email:
                id_ref = str(p.get("id_interno", "")).strip()
                meta = meta_data_por_id.get(id_ref, {}) if id_ref else {}

                titulo_mostrar = p.get("titulo_descriptivo") or meta.get("titulo") or "Sin título"
                ref = p.get("referencia") or meta.get("referencia") or ""
                link_web = meta.get("link") or p.get("link") or "#"
                justificacion = (p.get("justificacion", "") or "").strip()

                impacto = impacto_normalizer(p) if impacto_normalizer else "BAJO"
                categorias = categorias_normalizer(p) if categorias_normalizer else []

                autor_item = (meta.get("autor") or "").strip()
                autor_html = f'<div class="meta">Autor: <b>{autor_item}</b></div>' if (autor_item and autor_item.upper() != "S/D") else ""

                if impacto == "ALTO":
                    clase_badge = "badge alto"
                elif impacto == "MEDIO":
                    clase_badge = "badge medio"
                else:
                    clase_badge = "badge ref"

                cat_badges = "".join([f'<div class="badge ref">{c}</div>' for c in categorias])

                html += f"""
      <div class="item" role="article" aria-labelledby="title-{id_ref}">
        <div class="badges">
          <div class="badge ref">{ref}</div>
          <div class="{clase_badge}">Impacto {impacto}</div>
          {cat_badges}
        </div>
        <div id="title-{id_ref}" class="item-title">{titulo_mostrar}</div>
        {autor_html}
        <div class="meta">{meta.get("fuente", "") or p.get("source", "") or ""} • {p.get("fecha", p.get("date", ""))}</div>
        <div class="justificacion">{justificacion}</div>
        <a class="cta" href="{link_web}" target="_blank" rel="noopener">Ver Texto Oficial</a>
      </div>
"""
            # Link volver al índice (al final de la sección)
            html += '      <div class="back"><a href="#top">Volver al índice</a></div>\n'
            html += "    </div>\n"  

            # Agregar divider solo si NO es la última sección que se renderiza
            if idx < len(secciones_a_mostrar) - 1:
                html += f"    {self._section_divider()}\n"

        html += self.footer()
        return html
