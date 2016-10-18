set -e
cd ../vignettes/
for i in ash one_sample_location one_sample_location_python one_sample_location_winsor; do
    cd $i; rm -rf .sos
    for j in `ls *.dsc`; do
        dsc $j -j8 && dsc $j -j8
    done
    cd -
done
git clean -df
