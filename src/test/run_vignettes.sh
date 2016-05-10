cd ../../vignettes/
for i in ash one_sample_location one_sample_location_python one_sample_location_winsor; do
    cd $i
    for j in `ls *.dsc`; do
        dsc exec $j -j8 -f
    done
    cd -
done
