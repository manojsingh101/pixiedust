function setValue(id,attrvalue){
    var n = $(id);
    n.attr("value", attrvalue);
}
setValue("#pm_overallNumCores{{prefix}}",{{totalCores}});
setValue("#pm_overallNumExecutors{{prefix}}",{{numExecutors}});
