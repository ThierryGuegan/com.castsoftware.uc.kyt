

C_SHAPE="shape"
C_COLOR="color"
C_FILLCOLOR="fillcolor"

# Will output gviz node declaration
#   "<node name>" [ shape="<node shape>", color="<node color>", fillcolor="<node fill color>" ];
#  
def formatNodeDeclaration( aId, aLabel, aShape=None, aColor=None, aFillColor=None ):
    retVal = '"{}" [{} {} {} label={}];'.format(
        aId, _formatShape(aShape), _formatColor(aColor), _formatFillColor(aFillColor), aLabel
    )
    return retVal



def _formatNodeAttribute( aAttributeName, aAttributeValue ):
    if aAttributeValue:
        return '{}="{}",'.format(aAttributeName,aAttributeValue)
    else:
        return ""

def _formatShape( aShape ):
    return _formatNodeAttribute(C_SHAPE,aShape)

def _formatColor( aColor ):
    return _formatNodeAttribute(C_COLOR,aColor)

def _formatFillColor( aColor ):
    return _formatNodeAttribute(C_FILLCOLOR,aColor)
