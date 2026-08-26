# graph500-24.gnuplot
# Grouped horizontal bar chart: wall-time per algorithm per engine.
# Same style as cit-Patents.gnuplot, but the time axis is logarithmic:
# the data spans three orders of magnitude (92.7 s .. 43443 s) and a
# linear axis would render most bars invisible.
#
# Usage:
#   gnuplot graph500-24.gnuplot
#
# Output:
#   graph500-24.png

set terminal pngcairo size 1400,900 enhanced font "DejaVu Sans,24"
set output "graph500-24.png"

set title "graph500-24" font "DejaVu Sans,36"

set label 100 "8.9M nodes, 260.4M edges, M-size" \
    at graph 0.5, graph 1.05 center font "DejaVu Sans,22"

set xlabel "wall-time, seconds (log scale)"

# Horizontal bars: X carries the wall-time, Y the algorithm labels.
# Row 3 = wcc (top), 2 = pagerank, 1 = cdlp (bottom).
set yrange [0.5:3.5]
set ytics ("wcc" 3, "pagerank" 2, "cdlp" 1)

set logscale x 10
set xrange [50:200000]
set format x "%.0s%c"

set grid xtics

# Reserve enough bottom margin for the xlabel plus a bottom-center legend,
# and anchor the legend at the bottom-center of the image
# (bmargin placement alone lets it collide with the xlabel).
set bmargin 6
set key at screen 0.5, screen 0.015 center bottom horizontal box opaque

set style fill solid 1.0 border -1

# Group geometry: one row per algorithm, three engines per row.
row_gap  = 0.27   # vertical offset between engine bars within a group
bar_half = 0.12   # half-height of a single bar
x0       = 50     # bars start at the left edge of the (log) time axis

set style line 1 lc rgb "#EE6677"   # GraphFrames
set style line 2 lc rgb "#4477AA"   # CSR-based engine
set style line 3 lc rgb "#228833"   # DataFusion based engine

# Data: wall-time (seconds)  algorithm-row
# GraphFrames pagerank ended in Out-of-Memory: no bar, see label 101.
$graphframes << EOD
1241.0 3
43443.0 1
EOD

$csr << EOD
199.8 3
209.15 2
217.9 1
EOD

$datafusion << EOD
147.7 3
92.7 2
1015.17 1
EOD

# Zero-length GraphFrames pagerank bar: horizontal text instead.
set label 101 "Out-of-Memory" at first x0, first (2+row_gap) left offset 1,0 \
    font "DejaVu Sans,20" textcolor rgb "#EE6677"

# Bars: boxxyerrorbars spans x from the axis start to the wall-time.
# Wall-time labels at the end of each bar; a x1.8 multiplier is a fixed
# ~80px visual offset on this log axis, wide enough that the centered
# text stays clear of the bar end even for the widest label.
plot $graphframes using ($1):($2+row_gap):(x0):($1):($2+row_gap-bar_half):($2+row_gap+bar_half) \
         with boxxyerrorbars ls 1 title "GraphFrames", \
     $csr          using ($1):($2):(x0):($1):($2-bar_half):($2+bar_half) \
         with boxxyerrorbars ls 2 title "CSR*", \
     $datafusion   using ($1):($2-row_gap):(x0):($1):($2-row_gap-bar_half):($2-row_gap+bar_half) \
         with boxxyerrorbars ls 3 title "DataFusion*", \
     $graphframes using ($1*1.8):($2+row_gap):(sprintf("%.1f", $1)) \
         with labels font "DejaVu Sans,20" textcolor rgb "#EE6677" notitle, \
     $csr          using ($1*1.8):($2):(sprintf("%.1f", $1)) \
         with labels font "DejaVu Sans,20" textcolor rgb "#4477AA" notitle, \
     $datafusion   using ($1*1.8):($2-row_gap):(sprintf("%.1f", $1)) \
         with labels font "DejaVu Sans,20" textcolor rgb "#228833" notitle
