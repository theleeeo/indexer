package fetcher

type fetcher struct {
	resource string
	// client
}

type Manager struct {
	fetchers map[string]*fetcher
}

func NewManager() *Manager {
	return &Manager{}
}

// Signal a fetcher to fetch a resource by its type and ID.
func (f *Manager) FetchResource(resource string, resourceId string) error {

}
