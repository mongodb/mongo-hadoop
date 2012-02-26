$(function() {
    var load = function() {
        window.location = this.href;
    };
    var prev = function() {
        $("a.page.prev").first().each(load);
    };
    var next = function() {
        $("a.page.next").first().each(load);
    }
    $(document).keyup(function (event) {
        if (event.altKey || event.ctrlKey || event.shiftKey || event.metaKey)
            return;
        if (event.keyCode == 37) {
            prev();
        } else if (event.keyCode == 39) {
            next();
        }
    });
    var orig = null;
    var container = $(".container");
    var reset = function() {
        if (orig)
            container.animate({left: "0px"}, 200);
        orig = null;
    }
    var screenW = function() {
        return window.innerWidth;
    }
    var edge = function(touch) {
        return Math.abs(touch.screenX - screenW()/2) > (3*screenW() / 8);
    };
    document.body.ontouchstart = function(e){
        if(e.touches.length == 1 && 
           e.changedTouches.length == 1 &&
           edge(e.touches[0])
          ){
            orig = {
                screenX: e.touches[0].screenX,
                clientX: e.touches[0].clientX
            } // mobile safari reuses touch objects
            e.preventDefault();
        }
    };
    document.body.ontouchend = function(e){
        if(orig && e.touches.length == 0 && e.changedTouches.length == 1){
            var half = screenW() / 2;
            var touch = e.changedTouches[0];
            if (orig.screenX > half && touch.screenX < half
                && $(".next").length > 0) {
                container.animate(
                    {left: "-=" + half + "px"}, 100, "linear", next);
            } else if (orig.screenX < half && touch.screenX > half
                       && $(".prev").length > 0) {
                container.animate(
                    {left: "+=" + half + "px"}, 100, "linear", prev);
            } else {
                reset();
            }
        } else {
            reset();
        }
    };
    var test = 0;
    document.body.ontouchmove = function(e){
        if(e.touches.length == 1){
            var touch = e.touches[0];
            if (orig) {
                e.preventDefault();
                container[0].style.left = 
                    (touch.clientX - orig.clientX) + "px";
            }
        }
    };
    var show_message = "show table of contents";
    var hide_message = "hide table of contents";
    $(".collap").collapse({
        "head": "h4",
        show: function () {
            this.animate({ 
                height: "toggle"
            }, 300);
            this.prev(".toctitle").children("a").text(hide_message);
        },
        hide: function () {
            this.animate({
                height: "toggle"
            }, 300);
            this.prev(".toctitle").children("a").text(show_message); 
        }
    });
    $(".collap a.tochead").show();
    $(".collap a.tochead").click(function(event){
        $(".toctitle").children("a").click();
    });
    $(".collap .toctitle a").text(show_message);
});
