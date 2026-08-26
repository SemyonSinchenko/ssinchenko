# bench-combined.gnuplot
# Combined benchmark chart: three datasets stacked as rows in one image.
# Each panel title carries only the dataset size (nodes, edges, category);
# the dataset names live in the blog-post text, not in the image.
#   panel 1: cit-Patents     (linear time axis)
#   panel 2: graph500-24     (log time axis, marked)
#   panel 3: datagen-sf3k-fb (linear; GraphFrames & CSR are OOM everywhere)
# One shared engine legend sits at the bottom of the image; it is drawn
# with panel 1, which is the only panel plotting all three engines.
# Panel titles are two-line: dataset name (26pt) + size line (16pt).
#
# Usage:
#   gnuplot bench-combined.gnuplot
#
# Output:
#   bench-combined.png

set terminal pngcairo size 1400,1050 enhanced font "DejaVu Sans,18"
set output "bench-combined.png"

set style fill solid 1.0 border -1

# Group geometry: one row per algorithm, three engines per row.
row_gap  = 0.27   # vertical offset between engine bars within a group
bar_half = 0.12   # half-height of a single bar

set style line 1 lc rgb "#EE6677"   # GraphFrames
set style line 2 lc rgb "#4477AA"   # CSR-based engine
set style line 3 lc rgb "#228833"   # DataFusion based engine

# ------- data: cit-Patents (rows: 3=wcc, 2=pagerank, 1=cdlp) -------
$gf1 << EOD
118.90 3
129.62 2
164.47 1
EOD
$csr1 << EOD
13.14 3
13.86 2
15.07 1
EOD
$df1 << EOD
16.57 3
12.40 2
44.60 1
EOD

# ------- data: graph500-24 (GraphFrames pagerank: Out-of-Memory) -------
$gf2 << EOD
1241.0 3
43443.0 1
EOD
$csr2 << EOD
199.8 3
209.15 2
217.9 1
EOD
$df2 << EOD
147.7 3
92.7 2
1015.17 1
EOD

# ------- data: datagen-sf3k-fb (GraphFrames & CSR: OOM on all) -------
$df3 << EOD
2241.03 3
2203.2 2
9343.1 1
EOD

set multiplot layout 3,1

# =================== panel 1: cit-Patents (linear) ===================
set lmargin 11
set rmargin 5
set tmargin 2.5
set bmargin 2

set title "3.8M nodes, 16.5M edges, XS-size" font "DejaVu Sans,22"

unset logscale x
set xrange [0:185]
set format x "%g"
set yrange [0.5:3.5]
set ytics ("wcc" 3, "pagerank" 2, "cdlp" 1)
set grid xtics

# Shared engine legend for the whole image (anchored at the canvas bottom).
set key at screen 0.5, screen 0.012 center bottom horizontal box opaque

plot $gf1  using ($1):($2+row_gap):(0):($1):($2+row_gap-bar_half):($2+row_gap+bar_half) \
         with boxxyerrorbars ls 1 title "GraphFrames", \
     $csr1 using ($1):($2):(0):($1):($2-bar_half):($2+bar_half) \
         with boxxyerrorbars ls 2 title "CSR*", \
     $df1  using ($1):($2-row_gap):(0):($1):($2-row_gap-bar_half):($2-row_gap+bar_half) \
         with boxxyerrorbars ls 3 title "DataFusion*", \
     $gf1  using ($1+9):($2+row_gap):(sprintf("%.1f", $1)) \
         with labels font "DejaVu Sans,15" textcolor rgb "#EE6677" notitle, \
     $csr1 using ($1+9):($2):(sprintf("%.1f", $1)) \
         with labels font "DejaVu Sans,15" textcolor rgb "#4477AA" notitle, \
     $df1  using ($1+9):($2-row_gap):(sprintf("%.1f", $1)) \
         with labels font "DejaVu Sans,15" textcolor rgb "#228833" notitle

unset key

# =================== panel 2: graph500-24 (log) ===================
set tmargin 2.5
set bmargin 2

set title "8.9M nodes, 260.4M edges, M-size" font "DejaVu Sans,22"
set label 103 "log scale" at graph 0.98, graph 0.93 right \
    font "DejaVu Sans,14" textcolor rgb "#555555"

set logscale x 10
set xrange [50:200000]
set format x "%.0s%c"

# Zero-length GraphFrames pagerank bar: horizontal text instead.
set label 101 "Out-of-Memory" at first 50, first (2+row_gap) left offset 1,0 \
    font "DejaVu Sans,15" textcolor rgb "#EE6677"

plot $gf2  using ($1):($2+row_gap):(50):($1):($2+row_gap-bar_half):($2+row_gap+bar_half) \
         with boxxyerrorbars ls 1 notitle, \
     $csr2 using ($1):($2):(50):($1):($2-bar_half):($2+bar_half) \
         with boxxyerrorbars ls 2 notitle, \
     $df2  using ($1):($2-row_gap):(50):($1):($2-row_gap-bar_half):($2-row_gap+bar_half) \
         with boxxyerrorbars ls 3 notitle, \
     $gf2  using ($1*1.8):($2+row_gap):(sprintf("%.1f", $1)) \
         with labels font "DejaVu Sans,15" textcolor rgb "#EE6677" notitle, \
     $csr2 using ($1*1.8):($2):(sprintf("%.1f", $1)) \
         with labels font "DejaVu Sans,15" textcolor rgb "#4477AA" notitle, \
     $df2  using ($1*1.8):($2-row_gap):(sprintf("%.1f", $1)) \
         with labels font "DejaVu Sans,15" textcolor rgb "#228833" notitle

unset label 101
unset label 103

# ============== panel 3: datagen-sf3k-fb (linear) ==============
set tmargin 2.5
set bmargin 5

set title "33.4M nodes, 2.9B edges, XL-size" font "DejaVu Sans,22"

unset logscale x
set xrange [0:10500]
set format x "%.0s%c"
set xlabel "wall-time, seconds"

# GraphFrames and CSR died on every algorithm: annotate instead of bars.
set label 105 "GraphFrames: Out-of-Memory" at graph 0.97, graph 0.88 right \
    font "DejaVu Sans,16" textcolor rgb "#EE6677"
set label 106 "CSR-based: Out-of-Memory" at graph 0.97, graph 0.62 right \
    font "DejaVu Sans,16" textcolor rgb "#4477AA"

plot $df3 using ($1):($2-row_gap):(0):($1):($2-row_gap-bar_half):($2-row_gap+bar_half) \
         with boxxyerrorbars ls 3 notitle, \
     $df3 using ($1+400):($2-row_gap):(sprintf("%.1f", $1)) \
         with labels font "DejaVu Sans,15" textcolor rgb "#228833" notitle

unset label 105
unset label 106

unset multiplot
