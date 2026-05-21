# jvm_tax.gnuplot
# Usage:
#   gnuplot jvm_tax.gnuplot
#
# Output:
#   jvm_tax.png

set terminal pngcairo size 1200,630 enhanced font "DejaVu Sans,24"
set output "jvm_tax.png"

set title "Where Is the JVM Tax?" font "DejaVu Sans,44"

set label 100 "MulFloat64 over Arrow buffers: Java Vector API vs native reference" \
    at graph 0.5, graph 1.06 center font "DejaVu Sans,20"

set logscale x 2
set logscale y 10

set xrange [1024:1048576]
set yrange [0.7:10000]

set xlabel "Rows"
set ylabel "ops/ms"

set grid ytics xtics
set key top right box opaque

set format x "%.0s%c"

unset ytics
set ytics ("10^{0}" 1, "10^{1}" 10, "10^{2}" 100, "10^{3}" 1000, "10^{4}" 10000)

# Fake visual zero label for the cover image.
# Real 0 cannot exist on a log-scale axis.
# set label 200 "0" at graph -0.03, graph -0.015 right font "DejaVu Sans,20"

set style line 1 lw 5 pt 7 ps 1.7
set style line 2 lw 5 pt 5 ps 1.7

set label 1 "same performance class" at graph 0.06, graph 0.17 font "DejaVu Sans,24"
set label 2 "same buffers, same hardware" at graph 0.06, graph 0.11 font "DejaVu Sans,18"

plot '-' using 1:2 with linespoints ls 1 title "JVM", \
     '-' using 1:2 with linespoints ls 2 title "native"
1024 5906
16384 246
65536 38.1
1048576 0.78
e
1024 4830
16384 306
65536 46.3
1048576 0.88
e