//! [`edge_faded`] — a pass-through edge-fade wrapper. The upstream gpui rev
//! used by host apps (Aurin) has no `EdgeFade` primitive, so the wrapper keeps
//! its layout contract and simply paints its child.

use gpui::{
    AnyElement, App, Bounds, Element, GlobalElementId, InspectorElementId, IntoElement, LayoutId,
    Pixels, Window,
};

/// Fade the child's content at its own edges: `top`/`bottom` select which
/// edges (pass the "is there hidden overflow" flags), `band` is the ramp
/// height in px. Horizontal edges via [`EdgeFaded::fade_left`] /
/// [`EdgeFaded::fade_right`].
pub fn edge_faded(band: f32, top: bool, bottom: bool, child: impl IntoElement) -> EdgeFaded {
    EdgeFaded {
        band,
        top,
        bottom,
        left: false,
        right: false,
        child: child.into_any_element(),
    }
}

pub struct EdgeFaded {
    band: f32,
    top: bool,
    bottom: bool,
    left: bool,
    right: bool,
    child: AnyElement,
}

impl EdgeFaded {
    pub fn fade_left(mut self, on: bool) -> Self {
        self.left = on;
        self
    }

    pub fn fade_right(mut self, on: bool) -> Self {
        self.right = on;
        self
    }
}

impl Element for EdgeFaded {
    type RequestLayoutState = ();
    type PrepaintState = ();

    fn id(&self) -> Option<gpui::ElementId> {
        None
    }

    fn source_location(&self) -> Option<&'static core::panic::Location<'static>> {
        None
    }

    fn request_layout(
        &mut self,
        _id: Option<&GlobalElementId>,
        _inspector_id: Option<&InspectorElementId>,
        window: &mut Window,
        cx: &mut App,
    ) -> (LayoutId, ()) {
        (self.child.request_layout(window, cx), ())
    }

    fn prepaint(
        &mut self,
        _id: Option<&GlobalElementId>,
        _inspector_id: Option<&InspectorElementId>,
        _bounds: Bounds<Pixels>,
        _request_layout: &mut Self::RequestLayoutState,
        window: &mut Window,
        cx: &mut App,
    ) {
        self.child.prepaint(window, cx);
    }

    fn paint(
        &mut self,
        _id: Option<&GlobalElementId>,
        _inspector_id: Option<&InspectorElementId>,
        bounds: Bounds<Pixels>,
        _request_layout: &mut Self::RequestLayoutState,
        _prepaint: &mut Self::PrepaintState,
        window: &mut Window,
        cx: &mut App,
    ) {
        let _ = (
            self.band,
            self.top,
            self.bottom,
            self.left,
            self.right,
            bounds,
        );
        self.child.paint(window, cx);
    }
}

impl IntoElement for EdgeFaded {
    type Element = Self;

    fn into_element(self) -> Self::Element {
        self
    }
}
