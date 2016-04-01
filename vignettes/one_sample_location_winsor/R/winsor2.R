## move the datapoints that are x times the absolute deviations from mean
winsor2 <- function (x, multiple=3)
{
   if(length(multiple) != 1 || multiple <= 0) {
      stop("bad value for 'multiple'")
   }
   med <- median(x)
   y <- x - med
   sc <- mad(y, center=0) * multiple
   y[ y > sc ] <- sc
   y[ y < -sc ] <- -sc
   return(y + med)
}
x = winsor2(x, multiple)
