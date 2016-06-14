$(document).ready(function(){
    function close_accordion_section() {
        $('.accordion .accordion-section-title').removeClass('active');
        $('.accordion .accordion-section-content').slideUp(300).removeClass('open');
    }

    $('.accordion-section-title').click(function(e) {
        // Grab current anchor value
        var currentAttrValue = $(this).attr('href');

        if($(e.target).is('.active')) {
            close_accordion_section();
        }else {
            close_accordion_section();

            // Add active class to section title
            $(this).addClass('active');
            // Open up the hidden content panel
            $('.accordion ' + currentAttrValue).slideDown(300).addClass('open');
        }

        e.preventDefault();
    });

    $('.tabs .tab-links a').on('click', function(e)  {
        var currentAttrValue = jQuery(this).attr('href');

        // Show/Hide Tabs
        $('.tabs ' + currentAttrValue).show().siblings().hide();

        // Change/remove current tab to active
        $(this).parent('li').addClass('active').siblings().removeClass('active');

        e.preventDefault();
    });

    $(".fancybox-thumb").fancybox({
	prevEffect	: 'none',
	nextEffect	: 'none',
	helpers	: {
	    title	: {
		type: 'outside'
	    },
	    thumbs	: {
		width	: 50,
		height	: 50
	    }
	}
    });
	  $(".various").fancybox({
		    maxWidth	: 2000,
		    maxHeight	: 2000,
		    fitToView	: true,
		    width		: '90%',
		    height		: '90%',
		    autoSize	: true,
		    closeClick	: false,
		    openEffect	: 'none',
		    closeEffect	: 'none'
	  });
});
