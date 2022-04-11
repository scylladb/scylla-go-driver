package transport

type responseHandler chan response

const responseHandlerSize = 2

type responseHandlerAllocator struct {
	handlers []responseHandler
	size     uint
	idx      uint
}

func makeResponseHandlerAllocator(size uint) responseHandlerAllocator {
	return responseHandlerAllocator{
		handlers: make([]responseHandler, size),
		size:     size,
		idx:      0,
	}
}

// alloc reuses previously allocated handler or allocates new one.
func (r *responseHandlerAllocator) alloc() responseHandler {
	if r.idx > 0 {
		r.idx--
		return r.handlers[r.idx]
	}

	return make(responseHandler, responseHandlerSize)
}

// yield puts used handler back.
// After calling the ownership of responseHandler is passed to responseHandlerAllocator.
// If there is no room to store handler it is dropped.
func (r *responseHandlerAllocator) yield(h responseHandler) {
	if r.idx < r.size {
		r.handlers[r.idx] = h
		r.idx++
	}
}
