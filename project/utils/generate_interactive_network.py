import pandas as pd
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

print("Generating premium interactive network visualization...")

graph_path = "outputs/music_graph.parquet"
pagerank_path = "outputs/artist_pagerank.parquet"
labels_path = "outputs/music_labels.parquet"

if not os.path.exists(graph_path) or not os.path.exists(pagerank_path) or not os.path.exists(labels_path):
    print("Error: Missing required parquet files. Run previous steps first.")
    exit(1)

spark = SparkSession.builder.appName("MusicGenealogy_InteractiveViz").getOrCreate()

# Load PageRank
df_pagerank = spark.read.parquet(pagerank_path)
top_nodes = df_pagerank.orderBy(col("authority_score").desc()).limit(150)
pdf_nodes = top_nodes.toPandas()
top_artists = pdf_nodes["artist"].tolist()

# Load Clusters
df_labels = spark.read.parquet(labels_path)
artist_cluster_pd = df_labels.select("artist_name", "cluster_representative").dropDuplicates(["artist_name"]).toPandas()
cluster_map = dict(zip(artist_cluster_pd["artist_name"], artist_cluster_pd["cluster_representative"]))

# Load Graph Edges
df_graph = spark.read.parquet(graph_path)
edges = df_graph.filter(
    col("Original_Artist_Name").isin(top_artists)
    & col("Sampler_Artist_Name").isin(top_artists)
)
edges = (
    edges.groupBy(
        col("Sampler_Artist_Name").alias("source"),
        col("Original_Artist_Name").alias("target"),
    )
    .sum("weight")
    .withColumnRenamed("sum(weight)", "weight")
)
pdf_edges = edges.toPandas()
spark.stop()

# Build JSON Data
nodes = []
for idx, row in pdf_nodes.iterrows():
    cluster = cluster_map.get(row["artist"], "Unknown")
    nodes.append({"id": row["artist"], "pagerank": float(row["authority_score"]), "cluster": cluster})

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
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            background: #0b0a10;
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
            padding: 20px 32px;
            background: linear-gradient(180deg, rgba(11,10,16,0.95) 0%, rgba(11,10,16,0) 100%);
            pointer-events: none;
        }}
        .header h1 {{
            font-size: 22px; font-weight: 700; letter-spacing: -0.5px;
            background: linear-gradient(135deg, #ff8906, #e53170, #7f5af0);
            -webkit-background-clip: text; -webkit-text-fill-color: transparent;
            pointer-events: auto;
        }}
        .header .hint {{
            font-size: 13px; font-weight: 500; color: rgba(255,255,255,0.4); pointer-events: auto;
            background: rgba(255,255,255,0.05); padding: 6px 14px; border-radius: 20px;
        }}

        /* Tooltip */
        .tooltip {{
            position: absolute; pointer-events: none; opacity: 0;
            padding: 12px 18px; border-radius: 12px;
            background: rgba(20, 18, 28, 0.95);
            backdrop-filter: blur(16px); -webkit-backdrop-filter: blur(16px);
            border: 1px solid rgba(255,255,255,0.1);
            box-shadow: 0 10px 40px rgba(0,0,0,0.5);
            font-size: 14px; line-height: 1.5;
            transition: opacity 0.15s ease;
            max-width: 300px; z-index: 20;
        }}
        .tooltip b {{ color: #fff; font-size: 15px; display: block; margin-bottom: 4px; }}
        .tooltip .score {{ color: rgba(255,255,255,0.6); font-size: 12px; display: flex; align-items: center; gap: 6px; }}
        .tooltip .cluster-badge {{ 
            display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 10px; font-weight: 600; text-transform: uppercase; margin-top: 6px;
        }}

        /* Sliding Sidebar */
        #sidebar {{
            position: fixed; top: 0; bottom: 0; right: 0; z-index: 30;
            width: 380px; background: rgba(18, 16, 26, 0.85);
            backdrop-filter: blur(24px); -webkit-backdrop-filter: blur(24px);
            border-left: 1px solid rgba(255,255,255,0.08);
            box-shadow: -10px 0 50px rgba(0,0,0,0.6);
            transform: translateX(100%);
            transition: transform 0.4s cubic-bezier(0.2, 0.8, 0.2, 1);
            display: flex; flex-direction: column;
        }}
        #sidebar.visible {{ transform: translateX(0); }}
        
        .sidebar-header {{
            padding: 30px 24px 20px 24px; border-bottom: 1px solid rgba(255,255,255,0.05); position: relative;
        }}
        .close-btn {{
            position: absolute; top: 20px; right: 20px;
            background: rgba(255,255,255,0.1); border: none; color: #fff;
            width: 32px; height: 32px; border-radius: 50%;
            cursor: pointer; font-size: 16px; display: flex; align-items: center; justify-content: center;
            transition: background 0.2s;
        }}
        .close-btn:hover {{ background: rgba(255,255,255,0.2); }}
        
        #sb-name {{ font-size: 24px; font-weight: 700; margin-bottom: 6px; line-height: 1.2; }}
        #sb-cluster {{ display: inline-block; padding: 4px 10px; border-radius: 12px; font-size: 11px; font-weight: 600; text-transform: uppercase; margin-bottom: 16px; }}
        
        .stat-box {{ background: rgba(0,0,0,0.2); border-radius: 12px; padding: 12px 16px; margin-bottom: 10px; border: 1px solid rgba(255,255,255,0.03); }}
        .stat-label {{ font-size: 11px; color: rgba(255,255,255,0.5); text-transform: uppercase; font-weight: 600; margin-bottom: 4px; }}
        .stat-value {{ font-size: 18px; font-weight: 700; font-family: monospace; }}

        .sidebar-content {{ flex: 1; overflow-y: auto; padding: 0 24px 30px 24px; scrollbar-width: thin; scrollbar-color: rgba(255,255,255,0.1) transparent; }}
        
        .conn-section {{ margin-top: 24px; }}
        .conn-title {{ font-size: 12px; font-weight: 600; color: rgba(255,255,255,0.4); text-transform: uppercase; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }}
        .conn-title.out {{ color: #ff8906; }}
        .conn-title.in {{ color: #e53170; }}
        
        .conn-item {{ display: flex; align-items: center; justify-content: space-between; padding: 10px 12px; background: rgba(255,255,255,0.03); border-radius: 8px; margin-bottom: 6px; transition: background 0.2s; }}
        .conn-item:hover {{ background: rgba(255,255,255,0.06); cursor: pointer; }}
        .conn-name {{ font-size: 13px; font-weight: 500; display: flex; align-items: center; gap: 8px; }}
        .conn-dot {{ width: 8px; height: 8px; border-radius: 50%; }}
        .conn-weight {{ font-size: 11px; color: rgba(255,255,255,0.4); font-family: monospace; background: rgba(0,0,0,0.3); padding: 2px 6px; border-radius: 4px; }}

        /* Legend */
        .legend {{
            position: fixed; bottom: 30px; left: 30px; z-index: 10;
            padding: 20px; border-radius: 16px;
            background: rgba(18, 16, 26, 0.85);
            backdrop-filter: blur(16px);
            border: 1px solid rgba(255,255,255,0.08);
            font-size: 12px; color: rgba(255,255,255,0.7);
            min-width: 220px; box-shadow: 0 10px 30px rgba(0,0,0,0.4);
            pointer-events: none;
        }}
        .legend-title {{ font-weight: 700; margin-bottom: 12px; color: #fff; font-size: 14px; }}
        .legend-section {{ margin-top: 14px; padding-top: 14px; border-top: 1px solid rgba(255,255,255,0.06); }}
        
        .edge-legend {{ display: flex; align-items: center; gap: 10px; margin: 8px 0; }}
        .edge-line {{ flex-shrink: 0; width: 30px; height: 2px; }}
        .edge-solid {{ background: #7f5af0; }}
        .edge-dashed {{ border-top: 2px dashed rgba(255,255,255,0.3); height: 0; }}
        
        /* Node sizes in legend */
        .size-legend {{ display: flex; align-items: flex-end; gap: 12px; margin-top: 12px; }}
        .size-circle {{ border: 1px solid rgba(255,255,255,0.3); border-radius: 50%; display: flex; justify-content: center; align-items: center; }}
        .size-container {{ display: flex; flex-direction: column; align-items: center; gap: 6px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Music Genealogy Network <span style="font-weight:400;font-size:16px;color:rgba(255,255,255,0.5);margin-left:10px;">Top 150 Artists by Authority</span></h1>
        <span class="hint">Scroll to zoom · Drag to pan · Click to explore</span>
    </div>
    
    <div id="tooltip" class="tooltip"></div>
    
    <!-- Sleek Sidebar -->
    <div id="sidebar">
        <div class="sidebar-header">
            <button class="close-btn" onclick="clearSelection()">✕</button>
            <div id="sb-cluster"></div>
            <h2 id="sb-name"></h2>
            <div class="stat-box">
                <div class="stat-label">PageRank Authority</div>
                <div class="stat-value" id="sb-score"></div>
            </div>
        </div>
        <div class="sidebar-content">
            <div class="conn-section" id="sb-outgoing-section" style="display:none;">
                <div class="conn-title out">▼ Samples (Outgoing)</div>
                <div id="sb-outgoing"></div>
            </div>
            <div class="conn-section" id="sb-incoming-section" style="display:none;">
                <div class="conn-title in">▲ Sampled By (Incoming)</div>
                <div id="sb-incoming"></div>
            </div>
        </div>
    </div>
    
    <div class="legend">
        <div class="legend-title">Network Key</div>
        <div style="margin-bottom:6px; color:rgba(255,255,255,0.5);">Node Color = Genealogical Family</div>
        <div style="margin-bottom:6px; color:rgba(255,255,255,0.5);">Node Size = Authority (PageRank)</div>
        
        <div class="size-legend">
            <div class="size-container"><div class="size-circle" style="width:10px; height:10px;"></div><span style="font-size:10px">Low</span></div>
            <div class="size-container"><div class="size-circle" style="width:20px; height:20px;"></div><span style="font-size:10px">Med</span></div>
            <div class="size-container"><div class="size-circle" style="width:34px; height:34px;"></div><span style="font-size:10px">High</span></div>
        </div>

        <div class="legend-section">
            <div style="color:rgba(255,255,255,0.5); margin-bottom: 8px;">Connections (Sampler → Original)</div>
            <div class="edge-legend"><div class="edge-line edge-solid"></div> <span>Intra-cluster (Same Family)</span></div>
            <div class="edge-legend"><div class="edge-line edge-dashed"></div> <span style="color:rgba(255,255,255,0.4)">Inter-cluster (Bridge)</span></div>
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

        // SVG Filters for premium glow
        const defs = svg.append("defs");
        const filter = defs.append("filter").attr("id", "glow").attr("x", "-50%").attr("y", "-50%").attr("width", "200%").attr("height", "200%");
        filter.append("feGaussianBlur").attr("stdDeviation", "6").attr("result", "coloredBlur");
        const feMerge = filter.append("feMerge");
        feMerge.append("feMergeNode").attr("in", "coloredBlur");
        feMerge.append("feMergeNode").attr("in", "SourceGraphic");

        const g = svg.append("g");

        const zoom = d3.zoom()
            .scaleExtent([0.15, 6])
            .on("zoom", (event) => {{
                g.attr("transform", event.transform);
            }});
        svg.call(zoom);

        // Vibrant categorical color palette for clusters
        const vibrantColors = [
            "#ff8906", "#e53170", "#7f5af0", "#2cb67d", "#3da9fc", 
            "#ef4565", "#f25f4c", "#ffc933", "#a259ff", "#00ebc7",
            "#f9bc60", "#f25f4c", "#eebbc3", "#b8c1ec"
        ];
        
        // Extract unique clusters and map to colors
        const clusters = Array.from(new Set(data.nodes.map(d => d.cluster))).filter(c => c !== "Unknown");
        const colorScale = d3.scaleOrdinal()
            .domain(clusters)
            .range(vibrantColors);
            
        function getNodeColor(cluster) {{
            return cluster === "Unknown" ? "#666666" : colorScale(cluster);
        }}

        // Size scale (Square root for power law PageRank)
        const prExtent = d3.extent(data.nodes, d => d.pagerank);
        const sizeScale = d3.scaleSqrt()
            .domain([0, prExtent[1]])
            .range([5, 35]); // Larger max size for big hubs

        // Build adjacency index for interaction
        const linkedByIndex = {{}};
        data.links.forEach(l => {{
            linkedByIndex[l.source + "," + l.target] = true;
            linkedByIndex[l.target + "," + l.source] = true;
        }});
        function isConnected(a, b) {{
            return a === b || linkedByIndex[a.id + "," + b.id];
        }}

        // Arrow markers (colored by cluster)
        clusters.forEach(c => {{
            defs.append("marker")
                .attr("id", `arrow-${{c.replace(/[^a-zA-Z0-9]/g, "")}}`)
                .attr("viewBox", "0 -5 10 10").attr("refX", 18).attr("refY", 0)
                .attr("markerWidth", 6).attr("markerHeight", 6).attr("orient", "auto")
                .append("path").attr("d", "M0,-4L10,0L0,4").attr("fill", colorScale(c)).style("opacity", 0.7);
        }});
        // Gray arrow for inter-cluster
        defs.append("marker")
            .attr("id", "arrow-gray")
            .attr("viewBox", "0 -5 10 10").attr("refX", 18).attr("refY", 0)
            .attr("markerWidth", 5).attr("markerHeight", 5).attr("orient", "auto")
            .append("path").attr("d", "M0,-4L10,0L0,4").attr("fill", "rgba(255,255,255,0.2)");

        const simulation = d3.forceSimulation(data.nodes)
            .force("link", d3.forceLink(data.links).id(d => d.id).distance(120))
            .force("charge", d3.forceManyBody().strength(d => -150 - sizeScale(d.pagerank)*5))
            .force("center", d3.forceCenter(width / 2, height / 2))
            .force("collide", d3.forceCollide().radius(d => sizeScale(d.pagerank) + 8).iterations(2));

        const link = g.append("g")
            .selectAll("line")
            .data(data.links)
            .join("line")
            .attr("stroke", d => {{
                if(d.source.cluster === d.target.cluster && d.source.cluster !== "Unknown") {{
                    return getNodeColor(d.source.cluster);
                }}
                return "rgba(255,255,255,0.15)";
            }})
            .attr("stroke-opacity", d => (d.source.cluster === d.target.cluster) ? 0.6 : 0.4)
            .attr("stroke-width", d => Math.max(1, Math.sqrt(d.weight)))
            .attr("stroke-dasharray", d => (d.source.cluster === d.target.cluster && d.source.cluster !== "Unknown") ? null : "4,4")
            .attr("marker-end", d => {{
                if(d.source.cluster === d.target.cluster && d.source.cluster !== "Unknown") {{
                    return `url(#arrow-${{d.source.cluster.replace(/[^a-zA-Z0-9]/g, "")}})`;
                }}
                return "url(#arrow-gray)";
            }});

        const node = g.append("g")
            .selectAll("circle")
            .data(data.nodes)
            .join("circle")
            .attr("r", d => sizeScale(d.pagerank))
            .attr("fill", d => getNodeColor(d.cluster))
            .attr("stroke", "#0b0a10")
            .attr("stroke-width", 2)
            .style("cursor", "pointer")
            .style("filter", d => sizeScale(d.pagerank) > 20 ? "url(#glow)" : null) // Glow for huge hubs
            .call(drag(simulation));

        // Smart Labels (Dynamic Opacity)
        const label = g.append("g")
            .selectAll("text")
            .data(data.nodes)
            .join("text")
            .attr("dy", d => -sizeScale(d.pagerank) - 8)
            .attr("text-anchor", "middle")
            .text(d => d.id)
            .style("fill", "#ffffff")
            .style("font-size", d => sizeScale(d.pagerank) > 15 ? "13px" : "11px")
            .style("font-weight", d => sizeScale(d.pagerank) > 15 ? "600" : "500")
            .style("paint-order", "stroke")
            .style("stroke", "rgba(11,10,16,0.9)")
            .style("stroke-width", "4px")
            .style("opacity", d => sizeScale(d.pagerank) > 18 ? 1 : 0) // Only show huge nodes by default
            .style("pointer-events", "none")
            .style("transition", "opacity 0.2s");

        // Tooltip interaction
        const tooltip = d3.select("#tooltip");

        node.on("mouseover", (event, d) => {{
            if(!selectedNode) {{
                // Fade non-neighbors slightly
                node.style("opacity", o => isConnected(d, o) ? 1 : 0.3);
                link.style("opacity", l => (l.source === d || l.target === d) ? 1 : 0.1);
                // Show neighbor labels
                label.style("opacity", o => isConnected(d, o) ? 1 : (sizeScale(o.pagerank) > 18 ? 0.3 : 0));
            }}

            const cColor = getNodeColor(d.cluster);
            tooltip.transition().duration(100).style("opacity", 1);
            tooltip.html(`
                <b>${{d.id}}</b>
                <span class="cluster-badge" style="background: ${{cColor}}33; color: ${{cColor}}; border: 1px solid ${{cColor}}66">${{d.cluster}}</span>
                <div style="margin-top:8px; height:1px; background:rgba(255,255,255,0.1)"></div>
                <div style="margin-top:8px;" class="score">Authority: <strong style="color:#fff">${{d.pagerank.toFixed(4)}}</strong></div>
            `)
            .style("left", (event.pageX + 16) + "px")
            .style("top", (event.pageY - 40) + "px");
        }})
        .on("mousemove", (event) => {{
            tooltip.style("left", (event.pageX + 16) + "px")
                   .style("top", (event.pageY - 40) + "px");
        }})
        .on("mouseout", () => {{
            if(!selectedNode) {{
                node.style("opacity", 1);
                link.style("opacity", 1);
                label.style("opacity", d => sizeScale(d.pagerank) > 18 ? 1 : 0);
            }}
            tooltip.transition().duration(200).style("opacity", 0);
        }});

        // Click interaction (Sidebar)
        let selectedNode = null;

        window.simulateClick = function(id) {{
            const targetNode = data.nodes.find(n => n.id === id);
            if(targetNode) node.nodes().forEach(n => {{ if(n.__data__.id === id) n.dispatchEvent(new Event('click')); }});
        }};

        node.on("click", (event, d) => {{
            event.stopPropagation();
            selectedNode = d;

            // Highlight mode
            node.style("opacity", o => isConnected(d, o) ? 1 : 0.1);
            link.style("opacity", l => (l.source === d || l.target === d) ? 1 : 0.05)
                .attr("stroke-width", l => (l.source === d || l.target === d) ? Math.max(2, Math.sqrt(l.weight)*1.2) : Math.max(1, Math.sqrt(l.weight)));
            label.style("opacity", o => isConnected(d, o) ? 1 : 0);

            // Populate Sidebar
            const panel = document.getElementById("sidebar");
            const cColor = getNodeColor(d.cluster);
            
            document.getElementById("sb-name").textContent = d.id;
            const clusterElem = document.getElementById("sb-cluster");
            clusterElem.textContent = d.cluster;
            clusterElem.style.background = cColor + "33";
            clusterElem.style.color = cColor;
            clusterElem.style.border = `1px solid ${{cColor}}88`;
            
            document.getElementById("sb-score").textContent = d.pagerank.toFixed(4);

            // Connections
            const outgoing = data.links.filter(l => l.source.id === d.id).sort((a,b) => b.weight - a.weight);
            const incoming = data.links.filter(l => l.target.id === d.id).sort((a,b) => b.weight - a.weight);
            
            const renderConn = (arr, isOut) => arr.map(l => {{
                const targetId = isOut ? l.target.id : l.source.id;
                const tColor = getNodeColor(isOut ? l.target.cluster : l.source.cluster);
                return `
                <div class="conn-item" onclick="simulateClick('${{targetId.replace(/'/g, "\\'")}}')">
                    <div class="conn-name">
                        <div class="conn-dot" style="background:${{tColor}}"></div>
                        ${{targetId}}
                    </div>
                    <div class="conn-weight">${{l.weight}}</div>
                </div>`;
            }}).join("");

            if(outgoing.length > 0) {{
                document.getElementById("sb-outgoing-section").style.display = "block";
                document.getElementById("sb-outgoing").innerHTML = renderConn(outgoing, true);
            }} else {{ document.getElementById("sb-outgoing-section").style.display = "none"; }}

            if(incoming.length > 0) {{
                document.getElementById("sb-incoming-section").style.display = "block";
                document.getElementById("sb-incoming").innerHTML = renderConn(incoming, false);
            }} else {{ document.getElementById("sb-incoming-section").style.display = "none"; }}

            panel.classList.add("visible");
        }});

        svg.on("click", () => {{ clearSelection(); }});

        window.clearSelection = function() {{
            selectedNode = null;
            node.style("opacity", 1);
            link.style("opacity", 1).attr("stroke-width", d => Math.max(1, Math.sqrt(d.weight)));
            label.style("opacity", d => sizeScale(d.pagerank) > 18 ? 1 : 0);
            document.getElementById("sidebar").classList.remove("visible");
        }}

        simulation.on("tick", () => {{
            link.attr("x1", d => d.source.x).attr("y1", d => d.source.y)
                .attr("x2", d => d.target.x).attr("y2", d => d.target.y);
            node.attr("cx", d => d.x).attr("cy", d => d.y);
            label.attr("x", d => d.x).attr("y", d => d.y);
        }});

        function drag(simulation) {{
            function dragstarted(event) {{
                if (!event.active) simulation.alphaTarget(0.3).restart();
                event.subject.fx = event.subject.x; event.subject.fy = event.subject.y;
            }}
            function dragged(event) {{
                event.subject.fx = event.x; event.subject.fy = event.y;
            }}
            function dragended(event) {{
                if (!event.active) simulation.alphaTarget(0);
                event.subject.fx = null; event.subject.fy = null;
            }}
            return d3.drag().on("start", dragstarted).on("drag", dragged).on("end", dragended);
        }}
    </script>
</body>
</html>
"""

with open("outputs/interactive_genealogy.html", "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"✓ Premium interactive network saved to outputs/interactive_genealogy.html")
print(f"  Nodes: {len(nodes)}, Links: {len(links)}")
