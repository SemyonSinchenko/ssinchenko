# cit-Patents.gnuplot
# Grouped horizontal bar chart: wall-time per algorithm per engine.
#
# Usage:
#   gnuplot cit-Patents.gnuplot
#
# Output:
#   cit-Patents.png

set terminal pngcairo size 1400,900 enhanced font "DejaVu Sans,24"
set output "cit-Patents.png"

set title "cit-Patents" font "DejaVu Sans,36"

set label 100 "3.8M nodes, 16.5M edges, XS-size" \
    at graph 0.5, graph 1.05 center font "DejaVu Sans,22"

set xlabel "wall-time, seconds"

# Reserve enough bottom margin for the xlabel plus a bottom-center legend,
# and anchor the legend at the bottom-center of the image
# (bmargin placement alone lets it collide with the xlabel).
set bmargin 6
set key at screen 0.5, screen 0.015 center bottom horizontal box opaque

# Horizontal bars: X carries the wall-time, Y the algorithm labels.
# Row 3 = wcc (top), 2 = pagerank, 1 = cdlp (bottom).
set yrange [0.5:3.5]
set ytics ("wcc" 3, "pagerank" 2, "cdlp" 1)

set xrange [0:185]

set grid xtics

set style fill solid 1.0 border -1

# Group geometry: one row per algorithm, three engines per row.
row_gap  = 0.27   # vertical offset between engine bars within a group
bar_half = 0.12   # half-height of a single bar

set style line 1 lc rgb "#EE6677"   # GraphFrames
set style line 2 lc rgb "#4477AA"   # CSR-based engine
set style line 3 lc rgb "#228833"   # DataFusion based engine

# Data: wall-time (seconds)  algorithm-row
$graphframes << EOD
118.90 3
129.62 2
164.47 1
EOD

$csr << EOD
13.14 3
13.86 2
15.07 1
EOD

$datafusion << EOD
16.57 3
12.40 2
44.60 1
EOD

# Bars: boxxyerrorbars spans x from 0 to wall-time, y from row-gap to row+gap.
# Wall-time labels at the end of each bar.
plot $graphframes using ($1):($2+row_gap):(0):($1):($2+row_gap-bar_half):($2+row_gap+bar_half) \
         with boxxyerrorbars ls 1 title "GraphFrames", \
     $csr          using ($1):($2):(0):($1):($2-bar_half):($2+bar_half) \
         with boxxyerrorbars ls 2 title "CSR*", \
     $datafusion   using ($1):($2-row_gap):(0):($1):($2-row_gap-bar_half):($2-row_gap+bar_half) \
         with boxxyerrorbars ls 3 title "DataFusion*", \
     $graphframes using ($1+9):($2+row_gap):(sprintf("%.1f", $1)) \
         with labels font "DejaVu Sans,20" textcolor rgb "#EE6677" notitle, \
     $csr          using ($1+9):($2):(sprintf("%.1f", $1)) \
         with labels font "DejaVu Sans,20" textcolor rgb "#4477AA" notitle, \
     $datafusion   using ($1+9):($2-row_gap):(sprintf("%.1f", $1)) \
         with labels font "DejaVu Sans,20" textcolor rgb "#228833" notitle
