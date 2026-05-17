import pandas as pd
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

print("Generating interactive network visualization...")

graph_path = "outputs/music_graph.parquet"
pagerank_path = "outputs/artist_pagerank.parquet"

if not os.path.exists(graph_path) or not os.path.exists(pagerank_path):
    print("Error: Missing required parquet files. Run previous steps first.")
    exit(1)

spark = SparkSession.builder.appName("MusicGenealogy_InteractiveViz").getOrCreate()

df_pagerank = spark.read.parquet(pagerank_path)
top_nodes = df_pagerank.orderBy(col("authority_score").desc()).limit(150)
pdf_nodes = top_nodes.toPandas()

top_artists = pdf_nodes["artist"].tolist()

df_graph = spark.read.parquet(graph_path)
# Keep edges where both source and target are in top_artists
edges = df_graph.filter(
    col("Original_Artist_Name").isin(top_artists)
    & col("Sampler_Artist_Name").isin(top_artists)
)

edges = (
    edges.groupBy(
        col("Original_Artist_Name").alias("source"),
        col("Sampler_Artist_Name").alias("target"),
    )
    .sum("weight")
    .withColumnRenamed("sum(weight)", "weight")
)

pdf_edges = edges.toPandas()
spark.stop()

nodes = []
for idx, row in pdf_nodes.iterrows():
    nodes.append({"id": row["artist"], "pagerank": float(row["authority_score"])})

links = []
for idx, row in pdf_edges.iterrows():
    links.append(
        {
            "source": row["source"],
            "target": row["target"],
            "weight": float(row["weight"]),
        }
    )

graph_data = {"nodes": nodes, "links": links}

html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Music Genealogy Network</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            background: #0f0e17;
            color: #fffffe;
            font-family: 'Inter', system-ui, -apple-system, sans-serif;
            overflow: hidden;
        }}
        #network {{ width: 100vw; height: 100vh; display: block; cursor: grab; }}
        #network:active {{ cursor: grabbing; }}

        /* Header bar */
        .header {{
            position: fixed; top: 0; left: 0; right: 0; z-index: 10;
            display: flex; align-items: center; justify-content: space-between;
            padding: 16px 28px;
            background: linear-gradient(180deg, rgba(15,14,23,0.95) 0%, rgba(15,14,23,0) 100%);
            pointer-events: none;
        }}
        .header h1 {{
            font-size: 18px; font-weight: 700; letter-spacing: -0.3px;
            background: linear-gradient(135deg, #ff8906, #e53170);
            -webkit-background-clip: text; -webkit-text-fill-color: transparent;
            pointer-events: auto;
        }}
        .header .hint {{
            font-size: 12px; color: rgba(255,255,255,0.4); pointer-events: auto;
        }}

        /* Tooltip */
        .tooltip {{
            position: absolute; pointer-events: none; opacity: 0;
            padding: 12px 16px; border-radius: 10px;
            background: rgba(30, 28, 46, 0.92);
            backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);
            border: 1px solid rgba(255,255,255,0.08);
            box-shadow: 0 8px 32px rgba(0,0,0,0.4);
            font-size: 13px; line-height: 1.5;
            transition: opacity 0.15s ease;
            max-width: 280px;
        }}
        .tooltip b {{ color: #ff8906; }}
        .tooltip .score {{ color: rgba(255,255,255,0.5); font-size: 11px; }}

        /* Detail panel */
        #detail-panel {{
            position: fixed; bottom: 20px; left: 20px; z-index: 10;
            padding: 20px 24px; border-radius: 14px;
            background: rgba(30, 28, 46, 0.88);
            backdrop-filter: blur(16px); -webkit-backdrop-filter: blur(16px);
            border: 1px solid rgba(255,255,255,0.06);
            box-shadow: 0 12px 48px rgba(0,0,0,0.5);
            max-width: 340px; font-size: 13px;
            transform: translateY(20px); opacity: 0;
            transition: all 0.3s cubic-bezier(0.4,0,0.2,1);
            pointer-events: none;
        }}
        #detail-panel.visible {{
            transform: translateY(0); opacity: 1; pointer-events: auto;
        }}
        #detail-panel h2 {{
            font-size: 16px; font-weight: 700; margin-bottom: 8px;
            color: #ff8906;
        }}
        #detail-panel .connections {{
            margin-top: 10px; max-height: 180px; overflow-y: auto;
            scrollbar-width: thin; scrollbar-color: rgba(255,255,255,0.15) transparent;
        }}
        #detail-panel .conn-item {{
            padding: 4px 0; color: rgba(255,255,255,0.7); font-size: 12px;
            border-bottom: 1px solid rgba(255,255,255,0.04);
        }}
        #detail-panel .conn-item span {{ color: rgba(255,255,255,0.3); }}
        #detail-panel .close-btn {{
            position: absolute; top: 12px; right: 14px;
            background: none; border: none; color: rgba(255,255,255,0.3);
            cursor: pointer; font-size: 16px;
        }}
        #detail-panel .close-btn:hover {{ color: #fff; }}

        /* Legend */
        .legend {{
            position: fixed; bottom: 20px; right: 20px; z-index: 10;
            padding: 16px 20px; border-radius: 12px;
            background: rgba(30, 28, 46, 0.85);
            backdrop-filter: blur(12px);
            border: 1px solid rgba(255,255,255,0.1);
            font-size: 12px; color: rgba(255,255,255,0.7);
            min-width: 200px;
        }}
        .legend-title {{
            font-weight: 600; margin-bottom: 12px; color: #ff8906;
            font-size: 13px;
        }}
        .legend-item {{
            display: flex; align-items: center; gap: 10px; margin: 6px 0;
        }}
        .legend .dot {{
            width: 12px; height: 12px; border-radius: 50%;
            flex-shrink: 0;
        }}
        .legend-arrow {{
            display: flex; align-items: center; gap: 4px;
            margin-top: 10px; padding-top: 10px;
            border-top: 1px solid rgba(255,255,255,0.1);
        }}
        .legend-arrow svg {{ width: 48px; height: 16px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Music Genealogy Network <span style="font-weight:400;font-size:14px;color:rgba(255,255,255,0.5);margin-left:8px;">Top 150 Artists by Authority</span></h1>
        <span class="hint">Scroll to zoom · Drag to pan · Click a node to explore</span>
    </div>
    <div id="tooltip" class="tooltip"></div>
    <div id="detail-panel">
        <button class="close-btn" onclick="clearSelection()">✕</button>
        <h2 id="detail-name"></h2>
        <div id="detail-score"></div>
        <div class="connections" id="detail-connections"></div>
    </div>
    <div class="legend">
        <div class="legend-title">Node Size = Authority</div>
        <div class="legend-item"><span class="dot" style="background:#ff8906"></span> High authority</div>
        <div class="legend-item"><span class="dot" style="background:#e53170"></span> Medium authority</div>
        <div class="legend-item"><span class="dot" style="background:#7f5af0"></span> Low authority</div>
        <div class="legend-arrow">
            <svg viewBox="0 0 48 16">
                <text x="0" y="11" fill="#fffffe" font-size="9">A</text>
                <path d="M12,8 L32,8" stroke="rgba(255,136,6,0.8)" stroke-width="2" marker-end="url(#arrowhead-legend)"/>
                <text x="36" y="11" fill="#fffffe" font-size="9">B</text>
            </svg>
            <span style="font-size:10px; color:rgba(255,255,255,0.5);">A sampled B</span>
        </div>
        <div style="margin-top:6px; font-size:10px; color:rgba(255,255,255,0.4);">
            Arrow points from SAMPLER → ORIGINAL
        </div>
    </div>
    <svg id="network"></svg>

    <script>
        const data = {json.dumps(graph_data)};

        const width = window.innerWidth;
        const height = window.innerHeight;

        const svg = d3.select("#network")
            .attr("width", width)
            .attr("height", height);

        // Zoom container
        const g = svg.append("g");

        const zoom = d3.zoom()
            .scaleExtent([0.1, 8])
            .on("zoom", (event) => {{
                g.attr("transform", event.transform);
            }});
        svg.call(zoom);

        // Color scale: warm gradient from purple (low) through pink to orange (high)
        const prExtent = d3.extent(data.nodes, d => d.pagerank);
        const colorScale = d3.scaleSequential()
            .domain(prExtent)
            .interpolator(t => d3.interpolateRgb("#7f5af0", "#ff8906")(t));

        // Size scale
        const sizeScale = d3.scaleSqrt()
            .domain(prExtent)
            .range([4, 28]);

        // Build adjacency index for click interactions
        const linkedByIndex = {{}};
        data.links.forEach(l => {{
            linkedByIndex[l.source + "," + l.target] = true;
            linkedByIndex[l.target + "," + l.source] = true;
        }});
        function isConnected(a, b) {{
            return a === b || linkedByIndex[a.id + "," + b.id];
        }}

        // Arrow marker - more visible orange color
        const defs = g.append("defs");
        defs.append("marker")
            .attr("id", "arrowhead")
            .attr("viewBox", "0 -5 10 10")
            .attr("refX", 18)
            .attr("refY", 0)
            .attr("markerWidth", 7)
            .attr("markerHeight", 7)
            .attr("orient", "auto")
            .append("path")
            .attr("d", "M0,-4L10,0L0,4")
            .attr("fill", "rgba(255,136,6,0.6)");
        
        // Legend arrow marker
        defs.append("marker")
            .attr("id", "arrowhead-legend")
            .attr("viewBox", "0 -5 10 10")
            .attr("refX", 8)
            .attr("refY", 0)
            .attr("markerWidth", 5)
            .attr("markerHeight", 5)
            .attr("orient", "auto")
            .append("path")
            .attr("d", "M0,-4L10,0L0,4")
            .attr("fill", "rgba(255,136,6,0.8)");

        const simulation = d3.forceSimulation(data.nodes)
            .force("link", d3.forceLink(data.links).id(d => d.id).distance(90))
            .force("charge", d3.forceManyBody().strength(-220))
            .force("center", d3.forceCenter(width / 2, height / 2))
            .force("collide", d3.forceCollide().radius(d => sizeScale(d.pagerank) + 6));

        const link = g.append("g")
            .selectAll("line")
            .data(data.links)
            .join("line")
            .attr("stroke", "rgba(255,136,6,0.25)")
            .attr("stroke-width", d => Math.max(0.8, Math.sqrt(d.weight) * 0.7))
            .attr("marker-end", "url(#arrowhead)");

        const node = g.append("g")
            .selectAll("circle")
            .data(data.nodes)
            .join("circle")
            .attr("r", d => sizeScale(d.pagerank))
            .attr("fill", d => colorScale(d.pagerank))
            .attr("stroke", "rgba(255,255,255,0.15)")
            .attr("stroke-width", 1)
            .style("cursor", "pointer")
            .call(drag(simulation));

        // Labels: only show for larger nodes by default
        const label = g.append("g")
            .selectAll("text")
            .data(data.nodes)
            .join("text")
            .attr("dy", d => -sizeScale(d.pagerank) - 6)
            .attr("text-anchor", "middle")
            .text(d => d.id)
            .style("fill", "#fffffe")
            .style("font-size", d => {{
                const s = sizeScale(d.pagerank);
                return s > 12 ? "11px" : "9px";
            }})
            .style("font-weight", d => sizeScale(d.pagerank) > 16 ? "600" : "400")
            .style("paint-order", "stroke")
            .style("stroke", "#0f0e17")
            .style("stroke-width", "3px")
            .style("stroke-linecap", "round")
            .style("stroke-linejoin", "round")
            .style("opacity", d => sizeScale(d.pagerank) > 8 ? 1 : 0)
            .style("pointer-events", "none");

        // Tooltip
        const tooltip = d3.select("#tooltip");

        node.on("mouseover", (event, d) => {{
            tooltip.transition().duration(120).style("opacity", 1);
            tooltip.html(`<b>${{d.id}}</b><br><span class="score">Authority Score: ${{d.pagerank.toFixed(6)}}</span>`)
                .style("left", (event.pageX + 14) + "px")
                .style("top", (event.pageY - 36) + "px");
        }})
        .on("mousemove", (event) => {{
            tooltip.style("left", (event.pageX + 14) + "px")
                   .style("top", (event.pageY - 36) + "px");
        }})
        .on("mouseout", () => {{
            tooltip.transition().duration(300).style("opacity", 0);
        }});

        // Click to highlight neighbors
        let selectedNode = null;

        node.on("click", (event, d) => {{
            event.stopPropagation();
            selectedNode = d;

            // Fade everything
            node.transition().duration(300)
                .attr("opacity", o => isConnected(d, o) ? 1 : 0.08);
            link.transition().duration(300)
                .attr("stroke", l => (l.source === d || l.target === d) ? "rgba(255,136,6,0.8)" : "rgba(255,255,255,0.02)")
                .attr("stroke-width", l => (l.source === d || l.target === d) ? 2.5 : 0.3);
            label.transition().duration(300)
                .style("opacity", o => isConnected(d, o) ? 1 : 0);

            // Show detail panel
            const panel = document.getElementById("detail-panel");
            document.getElementById("detail-name").textContent = d.id;
            document.getElementById("detail-score").innerHTML = `<span style="color:rgba(255,255,255,0.5)">Authority: ${{d.pagerank.toFixed(6)}}</span>`;

            // Find connections - separate outgoing (samples) from incoming (sampled by)
            const conns = data.links.filter(l => l.source.id === d.id || l.target.id === d.id);
            const outgoing = conns.filter(l => l.source.id === d.id);
            const incoming = conns.filter(l => l.target.id === d.id);
            
            let connHTML = "";
            if (outgoing.length > 0) {{
                connHTML += "<div style='color:#ff8906;font-size:11px;font-weight:600;margin:8px 0 4px 0;'>▼ SAMPLES (outgoing)</div>";
                connHTML += outgoing.map(function(l) {{ return "<div class='conn-item'>→ " + l.target.id + " <span>(weight: " + l.weight + ")</span></div>"; }}).join("");
            }}
            if (incoming.length > 0) {{
                connHTML += "<div style='color:#e53170;font-size:11px;font-weight:600;margin:8px 0 4px 0;'>▲ SAMPLED BY (incoming)</div>";
                connHTML += incoming.map(function(l) {{ return "<div class='conn-item'>← " + l.source.id + " <span>(weight: " + l.weight + ")</span></div>"; }}).join("");
            }}
            document.getElementById("detail-connections").innerHTML = connHTML || "<div class='conn-item' style='color:rgba(255,255,255,0.3)'>No connections in top 150</div>";

            panel.classList.add("visible");
        }});

        svg.on("click", () => {{
            clearSelection();
        }});

        function clearSelection() {{
            selectedNode = null;
            node.transition().duration(300).attr("opacity", 1);
            link.transition().duration(300)
                .attr("stroke", "rgba(255,136,6,0.25)")
                .attr("stroke-width", d => Math.max(0.8, Math.sqrt(d.weight) * 0.7));
            label.transition().duration(300)
                .style("opacity", d => sizeScale(d.pagerank) > 8 ? 1 : 0);
            document.getElementById("detail-panel").classList.remove("visible");
        }}

        simulation.on("tick", () => {{
            link
                .attr("x1", d => d.source.x)
                .attr("y1", d => d.source.y)
                .attr("x2", d => d.target.x)
                .attr("y2", d => d.target.y);

            node
                .attr("cx", d => d.x)
                .attr("cy", d => d.y);

            label
                .attr("x", d => d.x)
                .attr("y", d => d.y);
        }});

        function drag(simulation) {{
            function dragstarted(event) {{
                if (!event.active) simulation.alphaTarget(0.3).restart();
                event.subject.fx = event.subject.x;
                event.subject.fy = event.subject.y;
            }}
            function dragged(event) {{
                event.subject.fx = event.x;
                event.subject.fy = event.y;
            }}
            function dragended(event) {{
                if (!event.active) simulation.alphaTarget(0);
                event.subject.fx = null;
                event.subject.fy = null;
            }}
            return d3.drag()
                .on("start", dragstarted)
                .on("drag", dragged)
                .on("end", dragended);
        }}
    </script>
</body>
</html>
"""

with open("outputs/interactive_genealogy.html", "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"✓ Interactive network saved to outputs/interactive_genealogy.html")
print(f"  Nodes: {{len(nodes)}}, Links: {{len(links)}}")
