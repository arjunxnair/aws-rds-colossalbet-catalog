# Databricks notebook source
# MAGIC %md
# MAGIC # Network visualization with D3.js
# MAGIC ---
# MAGIC One quick way to identify topics in tweets is to analyze the use of keywords or hashtags systematically. A common approach is to find pairs of hashtags (or words) that are often mentioned together in the same tweets. Visualizing the result such that pairs that often appear together are drawn close to each other gives us a visual way to explore a topic map.
# MAGIC
# MAGIC In this notebook, you'll learn how to quickly visualize networks using D3.js.
# MAGIC
# MAGIC You can find a good introduction to the force layout with D3.js here: <a href="https://www.d3indepth.com/force-layout/" target="_blank">Force layout on d3indepth.com</a>
# MAGIC
# MAGIC Find a collection of examples of what you can do with D3.js here: <a href="https://observablehq.com/@d3/gallery" target="_blank">D3.js gallery</a>

# COMMAND ----------

pip install networkx

# COMMAND ----------

# MAGIC %python
# MAGIC import json
# MAGIC df = spark.sql('select id, size, type from aws_rds_colossalbet.analytics.nodes')
# MAGIC nodes = df.toJSON().map(lambda j: json.loads(j)).collect()
# MAGIC #print(nodes)

# COMMAND ----------

# MAGIC %python
# MAGIC import json
# MAGIC df = spark.sql('select source, target, weight from aws_rds_colossalbet.analytics.edges')
# MAGIC edges = df.toJSON().map(lambda j: json.loads(j)).collect()
# MAGIC #print(edges)

# COMMAND ----------

# MAGIC %python
# MAGIC import networkx as nx
# MAGIC import pandas as pd
# MAGIC
# MAGIC nodes_df = pd.DataFrame(nodes)
# MAGIC edges_df = pd.DataFrame(edges)
# MAGIC
# MAGIC G = nx.from_pandas_edgelist(edges_df, 'source', 'target', ['weight'], create_using=nx.Graph())
# MAGIC
# MAGIC def find_interconnected_nodes(graph, start_node, depth):
# MAGIC     visited = set()
# MAGIC     queue = [(start_node, 0)]
# MAGIC
# MAGIC     interconnected_nodes = set()
# MAGIC
# MAGIC     while queue:
# MAGIC         current_node, current_depth = queue.pop(0)
# MAGIC
# MAGIC         if current_node not in visited:
# MAGIC             visited.add(current_node)
# MAGIC             interconnected_nodes.add(current_node)
# MAGIC
# MAGIC             if current_depth < depth:
# MAGIC                 neighbors = graph.neighbors(current_node)
# MAGIC                 queue.extend((neighbor, current_depth + 1) for neighbor in neighbors)
# MAGIC
# MAGIC     return list(interconnected_nodes)
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Example usage
# MAGIC start_node_id = dbutils.widgets.get("UserID")
# MAGIC interconnected_nodes = find_interconnected_nodes(G, start_node_id,2)
# MAGIC print("Interconnected Nodes:", interconnected_nodes)
# MAGIC
# MAGIC nodes_viz = [nodes for nodes in nodes if nodes['id'] in interconnected_nodes]
# MAGIC edges_viz = [edges for edges in edges if edges['target'] in interconnected_nodes]
# MAGIC
# MAGIC html = """
# MAGIC   <html>
# MAGIC   <head>
# MAGIC   <meta charset="utf-8" />
# MAGIC   <script src="https://d3js.org/d3-force.v1.min.js"></script>
# MAGIC   <script src="https://d3js.org/d3.v4.min.js"></script>
# MAGIC
# MAGIC </head>
# MAGIC   <body>
# MAGIC
# MAGIC     <div id="graphDiv"></div>
# MAGIC
# MAGIC <hr/>
# MAGIC
# MAGIC     <button onclick="download('png')">
# MAGIC       Download PNG
# MAGIC     </button>
# MAGIC
# MAGIC     <button onclick="download('jpg')">
# MAGIC       Download JPG
# MAGIC     </button>
# MAGIC
# MAGIC
# MAGIC  <script>
# MAGIC       var data = { nodes: %s, links: %s };
# MAGIC    
# MAGIC    
# MAGIC       // Get the maximum weight for an edge
# MAGIC       var maxWeight = 0;
# MAGIC       for (var i = 0; i < data.links.length; i++) {
# MAGIC         if (maxWeight < data.links[i].weight) maxWeight = data.links[i].weight;
# MAGIC       }
# MAGIC
# MAGIC       var height = 1000;
# MAGIC       var width = 1000;
# MAGIC       
# MAGIC
# MAGIC       // Append the canvas to the HTML document
# MAGIC       var graphCanvas = d3
# MAGIC         .select("#graphDiv")
# MAGIC         .append("canvas")
# MAGIC         .attr("width", width + "px")
# MAGIC         .attr("height", height + "px")
# MAGIC         .node();
# MAGIC
# MAGIC       var context = graphCanvas.getContext("2d");
# MAGIC
# MAGIC       var div = d3
# MAGIC         .select("body")
# MAGIC         .append("div")
# MAGIC         .attr("class", "tooltip")
# MAGIC         .style("opacity", 0);
# MAGIC
# MAGIC       var simulation = d3
# MAGIC         .forceSimulation()
# MAGIC         .force(
# MAGIC           "link",
# MAGIC           d3
# MAGIC             .forceLink()
# MAGIC             .id(function(d) {
# MAGIC               return d.id;
# MAGIC             })
# MAGIC             .distance(function(d) {
# MAGIC               return 2 * (maxWeight - d.weight);
# MAGIC             })
# MAGIC             .strength(function(d) {
# MAGIC               return 1 * (d.weight / maxWeight);;
# MAGIC             })
# MAGIC         )
# MAGIC         .force("charge", d3.forceManyBody().strength(-250))
# MAGIC         .force("center", d3.forceCenter(width / 2, height / 2))
# MAGIC         .force("x", d3.forceX(width / 2).strength(0.01))
# MAGIC         .force("y", d3.forceY(height / 2).strength(0.01))
# MAGIC         .alphaTarget(0)
# MAGIC         .alphaDecay(0.05);
# MAGIC
# MAGIC       var transform = d3.zoomIdentity;
# MAGIC
# MAGIC       initGraph(data);
# MAGIC
# MAGIC       function initGraph(tempData) {
# MAGIC         function zoomed() {
# MAGIC           console.log("zooming");
# MAGIC           transform = d3.event.transform;
# MAGIC           simulationUpdate();
# MAGIC         }
# MAGIC
# MAGIC         d3.select(graphCanvas)
# MAGIC           .call(
# MAGIC             d3
# MAGIC               .drag()
# MAGIC               .subject(dragsubject)
# MAGIC               .on("start", dragstarted)
# MAGIC               .on("drag", dragged)
# MAGIC               .on("end", dragended)
# MAGIC           )
# MAGIC           .call(
# MAGIC             d3
# MAGIC               .zoom()
# MAGIC               .scaleExtent([1 / 10, 8])
# MAGIC               .on("zoom", zoomed)
# MAGIC           );
# MAGIC
# MAGIC         function dragsubject() {
# MAGIC           var i,
# MAGIC             x = transform.invertX(d3.event.x),
# MAGIC             y = transform.invertY(d3.event.y),
# MAGIC             dx,
# MAGIC             dy;
# MAGIC           for (i = tempData.nodes.length - 1; i >= 0; --i) {
# MAGIC             node = tempData.nodes[i];
# MAGIC             dx = x - node.x;
# MAGIC             dy = y - node.y;
# MAGIC
# MAGIC             let radius = Math.min(30, node.size)
# MAGIC             if (dx * dx + dy * dy < radius * radius) {
# MAGIC               node.x = transform.applyX(node.x);
# MAGIC               node.y = transform.applyY(node.y);
# MAGIC
# MAGIC               return node;
# MAGIC             }
# MAGIC           }
# MAGIC         }
# MAGIC
# MAGIC         function dragstarted() {
# MAGIC           if (!d3.event.active) simulation.alphaTarget(0.3).restart();
# MAGIC           d3.event.subject.fx = transform.invertX(d3.event.x);
# MAGIC           d3.event.subject.fy = transform.invertY(d3.event.y);
# MAGIC         }
# MAGIC
# MAGIC         function dragged() {
# MAGIC           d3.event.subject.fx = transform.invertX(d3.event.x);
# MAGIC           d3.event.subject.fy = transform.invertY(d3.event.y);
# MAGIC         }
# MAGIC
# MAGIC         function dragended() {
# MAGIC           if (!d3.event.active) simulation.alphaTarget(0);
# MAGIC           d3.event.subject.fx = null;
# MAGIC           d3.event.subject.fy = null;
# MAGIC         }
# MAGIC
# MAGIC         simulation.nodes(tempData.nodes).on("tick", simulationUpdate);
# MAGIC
# MAGIC         simulation.force("link").links(tempData.links);
# MAGIC
# MAGIC         function render() {}
# MAGIC
# MAGIC         function simulationUpdate() {
# MAGIC           context.save();
# MAGIC
# MAGIC           context.clearRect(0, 0, width, height);
# MAGIC           context.translate(transform.x, transform.y);
# MAGIC           context.scale(transform.k, transform.k);
# MAGIC
# MAGIC           // Draw the links
# MAGIC           tempData.links.forEach(function(d) {
# MAGIC             context.beginPath();
# MAGIC             context.lineWidth = Math.min(d.weight, 40);
# MAGIC             context.strokeStyle = "rgba(0, 158, 227, .3)";
# MAGIC             context.moveTo(d.source.x, d.source.y);
# MAGIC             context.lineTo(d.target.x, d.target.y);
# MAGIC             context.stroke();
# MAGIC           });
# MAGIC
# MAGIC           // Draw the nodes
# MAGIC           tempData.nodes.forEach(function(d, i) {
# MAGIC             context.beginPath();
# MAGIC             context.arc(d.x, d.y, Math.min(30, d.size), 0, 2 * Math.PI, true);
# MAGIC             context.fillStyle = "rgba(0, 158, 227, 0.8)";
# MAGIC             context.fill();
# MAGIC             context.fillStyle = "rgba(0, 0, 0, 1)";
# MAGIC             context.fillText(d.id, d.x + 10, d.y);
# MAGIC           });
# MAGIC
# MAGIC           context.restore();
# MAGIC           
# MAGIC         }
# MAGIC       }
# MAGIC       
# MAGIC     function download(type) {
# MAGIC         var canvas = document.querySelector("canvas");
# MAGIC
# MAGIC         var imgUrl;
# MAGIC         
# MAGIC         if (type === "png") 
# MAGIC           imgUrl = canvas.toDataURL("image/png");
# MAGIC         else if (type === "jpg") 
# MAGIC           imgUrl = canvas.toDataURL("image/png");
# MAGIC
# MAGIC         window.open().document.write('<img src="' + imgUrl + '" />');
# MAGIC       }
# MAGIC     </script>
# MAGIC     
# MAGIC   </body>
# MAGIC </html>
# MAGIC """ % (nodes_viz, edges_viz)
# MAGIC
# MAGIC #print(html)
# MAGIC displayHTML(html)

# COMMAND ----------

[nodes for nodes in nodes if nodes['id'] in interconnected_nodes and nodes.get('type') == 'user']

# COMMAND ----------

[edges for edges in edges if edges['target'] in interconnected_nodes]

# COMMAND ----------


