import matplotlib
#the line below needs to be here so that matplotlib can save figures
#without an X server running - e.g. if using ssh/tmux
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.colors as colors
from matplotlib.ticker import LogLocator, MaxNLocator, FuncFormatter
from mpl_toolkits.axes_grid1 import make_axes_locatable
import argparse
import copy
import numpy as np
from scipy.optimize import differential_evolution, minimize
from scipy.ndimage import shift, rotate, map_coordinates
import scipy.interpolate
from scipy.stats import anderson
from astropy.io import fits
from astropy.convolution import convolve_fft
from astropy.wcs import WCS
from astroquery.simbad import Simbad
from astropy.time import Time
from astropy.coordinates import SkyCoord
import astropy.units as u
import pickle
import emcee
import corner
from multiprocessing import Pool
from collections import namedtuple
from enum import Enum
import warnings
import tqdm
import os


# from https://scipy-cookbook.readthedocs.io/items/Rebinning.html

def congrid(a, newdims, method='linear', centre=False, minusone=False):
    '''Arbitrary resampling of source array to new dimension sizes.
    Currently only supports maintaining the same number of dimensions.
    To use 1-D arrays, first promote them to shape (x,1).

    Uses the same parameters and creates the same co-ordinate lookup points
    as IDL''s congrid routine, which apparently originally came from a VAX/VMS
    routine of the same name.

    method:
    neighbour - closest value from original data
    nearest and linear - uses n x 1-D interpolations using
                         scipy.interpolate.interp1d
    (see Numerical Recipes for validity of use of n 1-D interpolations)
    spline - uses ndimage.map_coordinates

    centre:
    True - interpolation points are at the centres of the bins
    False - points are at the front edge of the bin

    minusone:
    For example- inarray.shape = (i,j) & new dimensions = (x,y)
    False - inarray is resampled by factors of (i/x) * (j/y)
    True - inarray is resampled by(i-1)/(x-1) * (j-1)/(y-1)
    This prevents extrapolation one element beyond bounds of input array.
    '''
    if not a.dtype in [np.float64, np.float32]:
        a = np.cast[float](a)

    m1 = np.cast[int](minusone)
    ofs = np.cast[int](centre) * 0.5
    old = np.array( a.shape )
    ndims = len( a.shape )
    if len( newdims ) != ndims:
        print( "[congrid] dimensions error. " \
              "This routine currently only support " \
              "rebinning to the same number of dimensions.")
        return None
    newdims = np.asarray( newdims, dtype=float )
    dimlist = []

    if method == 'neighbour':
        for i in range( ndims ):
            base = np.indices(newdims)[i]
            dimlist.append( (old[i] - m1) / (newdims[i] - m1) \
                            * (base + ofs) - ofs )
        cd = np.array( dimlist ).round().astype(int)
        newa = a[list( cd )]
        return newa

    elif method in ['nearest','linear']:
        # calculate new dims
        for i in range( ndims ):
            base = np.arange( newdims[i] )
            dimlist.append( (old[i] - m1) / (newdims[i] - m1) \
                            * (base + ofs) - ofs )
        # specify old dims
        olddims = [np.arange(i, dtype = np.float) for i in list( a.shape )]

        # first interpolation - for ndims = any
        mint = scipy.interpolate.interp1d( olddims[-1], a, kind=method )
        newa = mint( dimlist[-1] )

        #trorder = [ndims - 1] + range( ndims - 1 )
        trorder = [ndims - 1] + list(range( ndims - 1 ))
        for i in range( ndims - 2, -1, -1 ):
            newa = newa.transpose( trorder )

            mint = scipy.interpolate.interp1d( olddims[i], newa, kind=method )
            newa = mint( dimlist[i] )

        if ndims > 1:
            # need one more transpose to return to original dimensions
            newa = newa.transpose( trorder )

        return newa
    elif method in ['spline']:
        oslices = [ slice(0,j) for j in old ]
        oldcoords = np.ogrid[oslices]
        nslices = [ slice(0,j) for j in list(newdims) ]
        newcoords = np.mgrid[nslices]

        newcoords_dims = range(np.rank(newcoords))
        #make first index last
        newcoords_dims.append(newcoords_dims.pop(0))
        newcoords_tr = newcoords.transpose(newcoords_dims)
        # makes a view that affects newcoords

        newcoords_tr += ofs

        deltas = (np.asarray(old) - m1) / (newdims - m1)
        newcoords_tr *= deltas

        newcoords_tr -= ofs

        newa = map_coordinates(a, newcoords)
        return newa
    else:
        print( "Congrid error: Unrecognized interpolation type.\n", \
              "Currently only \'neighbour\', \'nearest\',\'linear\',", \
              "and \'spline\' are supported.")
        return None


### Classes ###

#A simple data structure intended to hold upper limits on model parameters
ParamLimits = namedtuple('ParamLimits', ['fmax', 'shiftmax', 'rmax',
                                         'imax', 'alphamin', 'alphamax',
                                         'drmax'])

class Plottable:
    """Class representing an astronomical image (either real or synthetic), with plotting functionality."""

    def __init__(self, pfov, image, image_hires = None, hires_scale = np.nan):
        self.pfov = pfov                #pixel field of view in arcsec
        self.image = image
        self.image_hires = image_hires  #optional high-resolution version of the image
        self.hires_scale = hires_scale


    def __sub__(self, other):
        """Return a basic Plottable object whose image is the difference of self and other.
        Note that the resulting object will not have an associated high-resolution image."""

        if not np.isclose(self.pfov, other.pfov):
            raise Exception("Tried to subtract two Plottables with different pixel sizes "
                            f"({self.pfov:.2f} / {other.pfov:.2f})")
        else:
            return Plottable(self.pfov, self.image - other.image)


    def _projected_sep_array(self, centre):
        """Make an array of projected separations from a particular point to each image pixel.

        Parameters
        ----------
        centre : 2D tuple of ints
            Indices of the point from which to calculate separations.

        Returns
        -------
        2D array
            Array of projected separations.
        """

        dx, dy = np.meshgrid(np.arange(self.image.shape[1]) - centre[1],
                             np.arange(self.image.shape[0]) - centre[0])

        return self.pfov * np.sqrt(dx**2 + dy**2)


    def consistent_gaussian(self, radius = np.nan):
        """Establish whether self.image appears consistent with Gaussian noise.

        If a radius is provided, return True if either the whole image, or the region within
        radius arcsec of the centre, is consistent with Gaussian noise. The idea here is that
        checking only the central region reduces the likelihood of other sources influencing
        the result, but reducing the number of pixels also increases the significance of any
        bright pixels, so it's useful to check both regions."""

        if not np.isnan(radius):
            sky_separation = self._projected_sep_array([i/2 for i in self.image.shape])
            data = self.image[sky_separation < radius]

        else:
            data = self.image.flatten()

        #perform an Anderson-Darling test for normality
        result = anderson(data)

        #according to scipy documentation, [2] should correspond to the 5% level,
        #however this function is set up to account for possible future changes
        sig = result.significance_level[2]
        crit = result.critical_values[2]

        #return the significance level and whether the data are consistent with a Gaussian at that level
        #cast as bool, otherwise will be np.bool_
        if np.isnan(radius):
            return sig, bool(result.statistic < crit)
        else:
            return sig, bool(result.statistic < crit or self.consistent_gaussian(np.nan)[1])


    def _find_brightest(self, sep_threshold, centre):
        """Find the brightest pixel within a specified angular distance of a given pixel in self.image.

        Parameters
        ----------
        sep_threshold : float
            Radius of search region in arcsec.
        centre : 2D tuple of ints
            Indices of centre of search region.

        Returns
        -------
        2D tuple of ints
            Indices of the brightest pixel.
        """

        sky_separation = self._projected_sep_array(centre)
        return np.unravel_index(np.ma.MaskedArray(self.image, sky_separation > sep_threshold).argmax(),
                                                  self.image.shape)


    def _get_limits(self):
        """Calculate the appropriate axis limits in arcsec for a plot of the object's image."""

        return [self.image.shape[1] * self.pfov / 2, -self.image.shape[1] * self.pfov / 2,
                -self.image.shape[0] * self.pfov / 2, self.image.shape[0] * self.pfov / 2]


    def plot(self, ax, plot_hires = False, xlabel = True, ylabel = True, log = False,
                   annotation = '', cmap_name = 'inferno', scalebar = False, dist = np.nan):
        """Plot self.image or self.image_hires using the provided axes, and add a colorbar below."""

        #change units from mJy/pix to mJy/arcsec^2
        intensity_scale = ((self.hires_scale if plot_hires else 1)/ self.pfov)**2

        image = self.image_hires if plot_hires else self.image

        if xlabel: ax.set_xlabel('$\mathregular{RA\ offset\ /\ arcsec}$')
        if ylabel: ax.set_ylabel('$\mathregular{Dec\ offset\ /\ arcsec}$')

        ax.tick_params(direction = 'in', color = 'white', width = 1, right = True, top = True)

        limits = self._get_limits()

        #plot NaN pixels (which will likely arise if plotting on a log scale) in black
        cmap = copy.copy(plt.cm.get_cmap(cmap_name))
        cmap.set_bad(color = 'k')

        im = ax.imshow(np.log10(image * intensity_scale) if log else image * intensity_scale,
                       origin = 'lower',
                       interpolation = 'none', cmap = cmap,
                       extent = limits)


        #put an annotation at the top left corner
        ax.annotate(annotation, xy = (0.05, 0.95), xycoords = 'axes fraction', color = 'white',
                    verticalalignment = 'top', horizontalalignment = 'left')

        #add a scalebar if desired
        if scalebar:
            if np.isnan(dist):
                warnings.warn("No distance provided to plot_image. Unable to plot a scale bar.",
                              stacklevel = 2)

            else:
                #place scalebar at the lower left corner
                scalebar_x = 0.05 #axis fraction
                scalebar_y = 0.05 #axis fraction

                #choose an appropriate scalebar length
                if dist < 5:
                    scalebar_au = 10
                elif dist < 1000:
                    scalebar_au = 100
                else:
                    scalebar_au = 1000

                scalebar_arcsec = scalebar_au / dist

                ax.plot([limits[0] + scalebar_x * (limits[1] - limits[0]),
                         limits[0] + scalebar_x * (limits[1] - limits[0]) - scalebar_arcsec],
                        [limits[2] + scalebar_y * (limits[3] - limits[2]) for i in range(2)],
                        color = 'white')

                ax.annotate(f'{scalebar_au} au',
                            xy = (scalebar_x + scalebar_arcsec / abs(limits[1] - limits[0]) + 0.02,
                                  scalebar_y),
                            xycoords = 'axes fraction', color = 'white',
                            verticalalignment = 'center', horizontalalignment = 'left')

        #add a colorbar
        if not log: cblabel = '$\mathregular{Intensity\ /\ (mJy\ arcsec^{-2})}$'
        else:       cblabel = '$\mathregular{log\ [\ Intensity\ /\ (mJy\ arcsec^{-2})\ ]}$'

        divider = make_axes_locatable(ax)
        cax = divider.append_axes('bottom', size = '5%', pad = 0.6)
        cb = plt.colorbar(im, cax = cax, orientation = 'horizontal')
        cb.set_label(cblabel)

        cb.ax.xaxis.set_major_formatter(FuncFormatter(lambda x , pos: f'{x:g}'))
        cb.ax.xaxis.set_minor_locator(plt.NullLocator())
        cb.ax.xaxis.set_major_locator(MaxNLocator(nbins = 5))

        # show SIMBAD sources
        source_coords = getattr(self, 'source_coords', [])
        source_names = getattr(self, 'source_names', [])
        for i, c in enumerate(source_coords):

            #need a half-pixel offset to align correctly with the image
            coord_x = limits[0] - self.pfov * (c[1] + 0.5)
            coord_y = limits[2] + self.pfov * (c[0] + 0.5)

            ax.plot([coord_x], [coord_y], c = 'tab:gray', marker = '+', markersize = 6)
            ax.annotate(source_names[i], xy = (coord_x, coord_y),
                        xytext = (2, 2), textcoords = 'offset points',
                        color = 'tab:gray')

        #return the relevant AxesImage
        return im


    def plot_contours(self, ax, rms, levels = [-3, -2, 2, 3], neg_col = 'gainsboro', pos_col = 'k'):
        """Plot contours showing the specified RMS levels of self.image."""

        limits = self._get_limits()

        #return the relevant QuadContourSet
        return ax.contour(np.linspace(limits[0], limits[1], self.image.shape[1]),
                          np.linspace(limits[2], limits[3], self.image.shape[0]),
                          self.image, [i * rms for i in levels],
                          colors = [neg_col, neg_col, pos_col, pos_col], linestyles = 'solid')


    def shifted(self, params):
        """Return a shifted and scaled copy of self.image."""

        return shift(self.image, [params[0], params[1]]) * params[2] / np.amax(self.image)


class ModelType(Enum):
    """Enumeration used to specify the method used for making a model image."""

    Geometric = 0
    Particle = 1


class Model(Plottable):
    """Class representing a disc model, with capability to make synthetic images of the modelled disc.
        
    Seven parameters, eight with unresolved flux, nine with alpha as well.
    """

    def __init__(self, params, shape, pfov, aupp, hires_scale, alpha, include_unres,
                 stellarflux, flux_factor, model_type = ModelType.Geometric, npart = 100000):
        """Store the defining properties of the model."""

        if model_type == ModelType.Geometric:
            self._flux_function = self._geometric_model
        elif model_type == ModelType.Particle:
            self._flux_function = self._particle_model
            self.npart = npart
        else:
            raise Exception(f"Invalid model type: {model_type}")

        (self.funres, self.fres, self.x0, self.y0,
         self.r1, self.r2, self.cosinc, self.theta) = params[:8] if include_unres else np.concatenate(([0], params[:7]))
        self.alpha = params[7+include_unres] if alpha is None else alpha

        self.pfov = pfov
        self.aupp = aupp
        self.hires_scale = hires_scale
        self.stellarflux = stellarflux
        self.flux_factor = flux_factor
        self.shape = shape
        self.model_type = model_type


    def _geometric_model(self):
        """Make a grid of model fluxes using the geometric method."""

        #note the use of +self.x0 but -self.y0, since RA increases to the left
        dx, dy = np.meshgrid(np.linspace(-self.aupp * self.shape[1] / 2, self.aupp * self.shape[1] / 2,
                                         num = self.shape[1] * self.hires_scale) + self.x0,
                             np.linspace(-self.aupp * self.shape[0] / 2, self.aupp * self.shape[0] / 2,
                                         num = self.shape[0] * self.hires_scale) - self.y0)

        #'primed' coordinates, i.e. in a frame rotated by theta
        dxpr = dx * np.cos(np.deg2rad(self.theta)) + dy * np.sin(np.deg2rad(self.theta))
        dypr = -dx * np.sin(np.deg2rad(self.theta)) + dy * np.cos(np.deg2rad(self.theta))

        #distances from the star to each model pixel
        r = np.sqrt(dypr**2 + (dxpr / self.cosinc)**2)

        #set disc flux based on r^-alpha profile
        in_disc = (r > self.r1) & (r < self.r2)
        flux = np.zeros(r.shape)
        flux[in_disc] = r[in_disc] ** -self.alpha

        #ensure we don't divide by zero (if there's no flux, we don't need to normalize anyway)
        #NOTE: normalization for the geometric model works differently to the particle model, in that
        #the flux within the image field of view, rather than the total flux in the disc, is set to fres.
        #these are equivalent as long as the disc lies entirely within the image, but not if it
        #extends partially outside the bounds.
        if np.sum(flux) > 0:
            flux = self.fres * flux / np.sum(flux)

        return flux


    def _particle_model(self):
        """Make a grid of model fluxes using the particle-based method."""

        #first get a variable uniformly distributed from 0-1
        u = np.random.uniform(size = self.npart)

        #transform from uniform distribution to linear distribution, so that the surface
        #number density of particles is constant across the disc (since it's n(r)/2*pi*r*dr)
        #see e.g. https://mathworld.wolfram.com/RandomNumber.html
        number_index = 1
        r = ((self.r2 ** (1 + number_index) - self.r1 ** (1 + number_index)) * u
             + self.r1 ** (1 + number_index)) ** (1 / (1 + number_index))

        #give the particles random azimuthal angles
        phi = np.random.uniform(0, 2 * np.pi, size = self.npart)

        #coordinates in the frame aligned with the major & minor axes
        dypr = r * np.cos(phi)
        dxpr = r * np.sin(phi) * self.cosinc

        #coordinates in the frame aligned with the image axes
        dx = dxpr * np.cos(np.deg2rad(self.theta)) - dypr * np.sin(np.deg2rad(self.theta)) - self.x0
        dy = dxpr * np.sin(np.deg2rad(self.theta)) + dypr * np.cos(np.deg2rad(self.theta)) + self.y0

        #bin the particles and weight them to give a r^-alpha profile that sums to self.fres
        flux = np.histogram2d(dy, dx,
                              weights = (self.fres / np.sum(r ** -self.alpha)) * r ** -self.alpha,
                              bins = [np.linspace(-self.shape[i] * self.aupp / 2,
                                                  self.shape[i] * self.aupp / 2,
                                                  self.shape[i] * self.hires_scale + 1) for i in range(2)])[0]

        return flux


    def _make_hires(self):
        """Store a high-res image without the central bright pixel in self.image_hires, and
        return a grid of fluxes including that pixel, calculated using the appropriate method."""

        flux = self._flux_function()

        #scale down for lost flux
        flux /= self.flux_factor

        #at this point, store the flux without the central bright pixel in self.image_hires
        #to improve contrast if plotted later
        self.image_hires = flux.copy()

        #central pixel gets additional flux from star plus any unresolved flux
        flux[int((self.shape[0] * self.hires_scale / 2)
                 + self.y0/(self.aupp/self.hires_scale)),
             int((self.shape[1] * self.hires_scale / 2)
                 - self.x0/(self.aupp/self.hires_scale))] += (self.stellarflux + self.funres) / self.flux_factor

        return flux


    def make_images(self, psf):
        """Store appropriate images for analysis/plotting in self.image and self.image_hires.

        Parameters
        ----------
        psf : Plottable
            PSF used to make synthetic observation. Must have an image_hires at the same scale as self.image_hires.
        """

        if self.hires_scale != psf.hires_scale:
            raise Exception(f"Model and PSF scales do not match ({self.hires_scale:.2f} / {psf.hires_scale:.2f})")

        #convolve high-resolution model with high-resolution PSF; note that the call to
        #_make_hires stores self.image_hires
        convolved_hires = convolve_fft(self._make_hires(), psf.image_hires)

        #rebin to lower-resolution image pixel size
        self.image = self.hires_scale**2 * congrid(convolved_hires, self.shape)


class Observation(Plottable):
    """Class representing a PACS observation. Stores the image and important associated metadata."""

    def __init__(self, filename, search_radius = 5, target_ra = np.nan, target_dec = np.nan, dist = np.nan,
                 boxsize = 13, hires_scale = 1, rotate_to = np.nan, normalize = False, psf = None,
                 query_simbad = False):
        """Load in an image, store some important parameters and perform initial image processing."""

        with fits.open(filename) as fitsfile:
            self.image = fitsfile['image'].data * 1000              #image in mJy/pixel
            self.pfov = fitsfile['image'].header['CDELT2'] * 3600   #pixel FOV in arcsec
            self.wav = int(fitsfile['PRIMARY'].header['WAVELNTH'])  #wavelength of observations
            self.level = int(fitsfile['PRIMARY'].header['LEVEL'])   #processing level (20 or 25)
            self.name = fitsfile['PRIMARY'].header['OBJECT']        #target name
            self.angle = fitsfile['PRIMARY'].header['POSANGLE']     #pointing position angle

            #extract the obsid; the appropriate keyword seemingly depends on the processing level
            try:
                self.obsid = fitsfile['PRIMARY'].header['OBSID001'] #this works for level 2.5
            except KeyError:
                self.obsid = fitsfile['PRIMARY'].header['OBS_ID']   #and this for level 2

            #get the expected star coordinates in pixels, if RA and dec were provided;
            #otherwise, assume it's at the centre of the image
            if np.isnan(target_ra) or np.isnan(target_dec):
                star_expected = [i / 2 for i in self.image.shape]
            else:
                wcs = WCS(fitsfile['image'].header)
                star_expected = np.flip(wcs.wcs_world2pix([[target_ra, target_dec]], 0)[0])

            #extract coverage level, so that we can estimate the rms flux in a suitable region
            cov = fitsfile['coverage'].data

        #refuse to analyse 160 micron data (70/100 is always available and generally at higher S/N)
        if self.wav != 70 and self.wav != 100:
            raise Exception(f"Please provide a 70 or 100 μm image ({filename} is at {self.wav} μm)")

        #factors to correct for flux lost during high-pass filtering (see Kennedy et al. 2012)
        if self.wav == 70:
            self.flux_factor = 1.16
        elif self.wav == 100:
            self.flux_factor = 1.19

        #if no distance is supplied, simply set d = 1 pc so that separations will be in arcsec, not au;
        #in_au can be stored in any saved output for future reference, and plots can be annotated with sep_unit,
        #which is intended to be embedded in a LaTeX string
        if np.isnan(dist):
            distance_provided = False
            dist = 1
            self.sep_unit = r'^{\prime\prime}'
            self.in_au = False
        else:
            distance_provided = True
            self.sep_unit = r'\mathrm{au}'
            self.in_au = True

        #au per pixel at the distance of the target
        self.aupp = self.pfov * dist

        #clean up NaN pixels
        self.image[np.isnan(self.image)] = 0
        cov[np.isnan(cov)] = 0

        #find the coordinates of the brightest pixel within search_radius arcsec
        #of the specified RA and dec (or simply the centre)
        brightest_pix = self._find_brightest(search_radius, star_expected)

        #estimate the rms flux in a region defined by two conditions: coverage is above a
        #specified level, and projected separation from the brightest pixel is above a certain level.
        #NOTE: if the provided RA/dec are far from the image centre, the region defined by these
        #conditions may not be the most appropriate (however, it's unlikely that we will be trying
        #to fit a source near the edge of a map)

        cov_threshold_rms = 0.6 #fraction of max coverage
        sep_threshold_rms = 15 #arcsec
        sky_separation = self._projected_sep_array(brightest_pix)

        bg_condition = (cov > cov_threshold_rms * np.max(cov)) & \
                       (sky_separation > sep_threshold_rms)
        self.rms, self.median = self._estimate_background(bg_condition)
                                                          
        #subtract the median from the image
        self.image -= self.median
        
        # if given a PSF object, compute the rms for point sources
        # use this to estimate pixel rms using beam area
        if psf:
            print('Generating PSF fit uncertainty map...')
            self.psffit_map = self._point_source_uncertainty(psf, condition=bg_condition)
            tmp = copy.copy(self)
            tmp.image = self.psffit_map
            self.psffit_rms, _ = tmp._estimate_background()
            self.psffit_rms *= self.flux_factor
            self.uncert = self.psffit_rms * np.sqrt(np.max(psf.image/np.sum(psf.image)))

        #need to scale up uncertainties since noise is correlated
#        natural_pixsize = 3.2 #always the case for PACS 70/100 micron images
#        self.uncert = self.rms * self._correlated_noise_factor(natural_pixsize)


        if np.isnan(rotate_to):
            #no rotation requested; simply crop down to the requested size
            self._crop_image(brightest_pix,  boxsize)

        else:
            #cut out a portion of the image with the brightest pixel at the centre - this step is necessary
            #because after the rotation the brightest_pix coordinates will no longer be correct
            self._crop_image(brightest_pix, 2 * boxsize)

            #rotate to the requested position angle (necessary if using image as a PSF)
            self.image = rotate(self.image, self.angle - rotate_to)

            #now cut down to the requested size; note that we again look for the brightest pixel
            #and put this in the centre, since the rotation may have introduced a small offset
            self._crop_image(self._find_brightest(2 * self.pfov, [i / 2 for i in self.image.shape]), boxsize)

        #normalize if requested
        if normalize: self.image /= np.sum(self.image)

        #rebin to a higher resolution if requested
        self.hires_scale = hires_scale
        if self.hires_scale >= 1:
            self.image_hires = congrid(self.image,
                                      [i * self.hires_scale for i in self.image.shape],
                                      minusone = True)

            #ensure that flux is conserved
            self.image_hires *= np.sum(self.image) / np.sum(self.image_hires)

        else:
            raise Exception(f"hires_scale should be an integer >= 1")


        if query_simbad:
            if not np.isnan(rotate_to):
                warnings.warn(f"SIMBAD source overplotting for rotated images is"
                              " not supported. Skipping query.", stacklevel = 2)

            else:
                self.source_coords=[]
                self.source_names=[]
                Simbad.add_votable_fields('pm','plx')
                with fits.open(filename) as fitsfile:
                    qra = fitsfile['PRIMARY'].header['RA'] if np.isnan(target_ra) else target_ra
                    qdec = fitsfile['PRIMARY'].header['DEC'] if np.isnan(target_dec) else target_dec
                    coord = SkyCoord(ra = qra, dec = qdec, unit = (u.degree, u.degree), frame = 'icrs')

                    #find sources within a circle whose radius is half the cutout side length
                    r = Simbad.query_region(coordinates = coord, radius = boxsize * self.pfov * u.arcsec)

                    if len(r) > 0:
                        wcs = WCS(fitsfile['image'].header)
                        for i in range(len(r)):
                            #assume a distance of 50pc if none is available
                            qdist = 50 * u.pc

                            #preferentially use the supplied distance
                            if distance_provided: qdist = dist * u.pc

                            #otherwise, try to get one from SIMBAD
                            elif r[i]['PLX_VALUE'] > 0: distance = (1e3 / r[i]['PLX_VALUE']) * u.pc

                            if np.isfinite(r[i]['PMRA']) and np.isfinite(r[i]['PMDEC']):
                                #J2000 sky coordinates
                                s2000 = SkyCoord(r[i]['RA'].replace(' ',':')+' '+r[i]['DEC'].replace(' ',':'),
                                                 unit = (u.hour, u.degree), distance = qdist,
                                                 pm_ra_cosdec = r[i]['PMRA'] * u.mas / u.yr,
                                                 pm_dec = r[i]['PMDEC'] * u.mas / u.yr,
                                                 obstime = Time(2451545.0,format='jd'))

                                #apply proper motion correction to observation date
                                s = s2000.apply_space_motion(new_obstime = Time(fitsfile['PRIMARY'].header['DATE-OBS']))

                            else:
                                s = SkyCoord(r[i]['RA'].replace(' ',':')+' '+r[i]['DEC'].replace(' ',':'),
                                             unit = (u.hour, u.degree))

                            #find the pixel corresponding to source i
                            coord = np.flip(wcs.wcs_world2pix([[s.ra.deg, s.dec.deg]], 0)[0])

                            #translate into image cutout coordinates
                            coord -= np.array(brightest_pix) - boxsize

                            #store the coordinates and source name as an attribute
                            append = True

                            #don't append duplicate coordinates (i.e. planets)
                            for c in self.source_coords:
                                if np.isclose(c, coord).all():
                                    append = False

                            if append:
                                self.source_coords.append(coord)
                                self.source_names.append(r[i]['MAIN_ID'].decode())


    def best_psf_subtraction(self, psf, param_limits, x0=0.0, y0=0.0, negative_flux=False):
        """Return the best-fitting PSF-subtracted image."""

        if not np.isclose(self.pfov, psf.pfov):
            raise Exception("best_psf_subtraction received a PSF with the wrong pixel size "
                            f"({self.pfov:.2f} / {psf.pfov:.2f})")

        #note that shiftmax here is in PACS pixels
        limits = [(x0-param_limits.shiftmax, x0+param_limits.shiftmax), #x shift
                  (y0-param_limits.shiftmax, y0+param_limits.shiftmax), #y shift
                  (0, 2 * np.amax(self.image))]                         #peak flux

        if negative_flux:
            limits[2] = (-2 * np.amax(self.image), 2 * np.amax(self.image))

        result = differential_evolution(lambda p: np.sum(((self.image - psf.shifted(p)) / self.uncert) ** 2),
                                        limits)

        return Plottable(image = self.image - psf.shifted(result['x']), pfov = self.pfov)


    def _gaussian_image(self, p):
        '''Return a 2d Gaussian with same dimensions as image.'''
        x0,y0,w1,w2,th,pk = p
        x0, y0 = x0/self.pfov, y0/self.pfov # convert to arcsec
        w1, w2 = w1/self.pfov, w2/self.pfov
        xx, yy = np.meshgrid(np.arange(self.image.shape[1]) - self.image.shape[1]/2,
                             np.arange(self.image.shape[0]) - self.image.shape[0]/2)
        x = xx * np.cos(th) - yy * np.sin(th)
        y = xx * np.sin(th) + yy * np.cos(th)
        return pk * np.exp( -0.5*((x-x0)**2 / (w1/2.35)**2 + \
                                  (y-y0)**2 / (w2/2.35)**2) )


    def best_gauss_fit(self, param_limits, x0=0.0, y0=0.0):
        '''Find best fitting 2d Gaussian, lengths in arcsec.'''
        
        #distances in pixels
        limits = [(x0-param_limits.shiftmax, x0+param_limits.shiftmax), #x shift
                  (y0-param_limits.shiftmax, y0+param_limits.shiftmax), #y shift
                  (4, 30), (4, 30), (0, np.pi/2),                       #
                  (0, 2 * np.amax(self.image))]                         #peak flux
    
        result = differential_evolution(lambda p: np.sum(((self.image - self._gaussian_image(p)) / self.uncert) ** 2),
                                    limits)
                                    
        if result['x'][2] < result['x'][3]:
            res = result['x'][[0,1,3,2,4,5]]
            res[4] += np.pi/2
            return res
        else:
            return result['x']


    def _crop_image(self, centre, boxscale):
        """Crop self.image such that the specified pixel is perfectly centred.

        Parameters
        ----------
        centre : 2D tuple of ints
            Indices of pixel to place at the centre.
        boxscale : int
            Cut out a square of dimension (2 * boxscale + 1).
        """

        #this function guarantees that the desired pixel will be exactly at the centre
        #as long as boxscale < centre[i], which is useful for making centred plots

        self.image = self.image[int(centre[0] - boxscale) : int(centre[0] + boxscale + 1),
                                int(centre[1] - boxscale) : int(centre[1] + boxscale + 1)]



    def _estimate_background(self, condition = None, sigma_level = 3.0, tol = 1e-6, max_iter = 20):
        """Estimate the background RMS of self.image or self.image[condition] using an iterative method.

        Parameters
        ----------
        condition : 2D array, optional
            Boolean array specifying which pixels of self.image to process. (default: None)
        sigma_level : float
            Number of standard deviations used to define outliers. (default: 3.0)
        tol : float, optional
            Fractional difference between RMS iterations at which to cease iterating. (default: 1e-6)
        max_iter : int, optional
            Maximum number of iterations. (default: 20)

        Returns
        -------
        float
            Estimated background RMS.
        float
            Estimated background median.
        """

        data = self.image.flatten() if condition is None else self.image[condition]
        rms = np.std(data)
        median = np.median(data)

        #arbitrarily set error to >tol so that the iterations can get started
        err = 2 * tol

        i = 0
        while (err > tol) and (i < max_iter):
            rmsprev = rms

            #discard data classified as outliers
            data = data[data < (sigma_level * rms + median)]
            rms = np.std(data)
            median = np.median(data)

            err = abs((rms - rmsprev) / rmsprev)
            i += 1

        if (i == max_iter) and (err > tol):
            warnings.warn(f"_estimate_background did not converge after {max_iter} iterations."
                          " You may wish to check the image for issues.", stacklevel = 2)

        return rms, median


    def _point_source_uncertainty(self, psf, condition = None):
        """Calculate an uncertainty map for point sources.
        
        PSF image is padded with zeros to size of self.image
        If condition is set, the output will not be reshaped into a map.
        """
    
        imsz = self.image.shape
        psfsz = psf.image.shape
        psf_tmp = copy.copy(psf)
        if psfsz[0] < imsz[0]:
            psf_tmp_im = np.zeros(self.image.shape)
            diffx2 = (imsz[0] - psfsz[0])//2
            diffy2 = (imsz[1] - psfsz[1])//2
            psf_tmp_im[diffx2:diffx2+psfsz[0], diffy2:diffy2+psfsz[1]] = psf.image
            psf_tmp.image = psf_tmp_im
    
        if condition is not None:
            xs, ys = np.where(condition)
        else:
            y,x = np.meshgrid(np.arange(self.image.shape[1]),
                              np.arange(self.image.shape[0]))
            xs = x.reshape(-1)
            ys = y.reshape(-1)
        
        with Pool() as pool:
            flux = pool.starmap(fit_one_psf, zip(np.repeat(self,len(xs)),
                                                 np.repeat(psf_tmp,len(xs)),
                                                 xs,ys))

        if condition is not None:
            return np.array(flux)
        else:
            return np.array(flux).reshape(self.image.shape)


    def _correlated_noise_factor(self, natural_pixsize):
        """Calculate the uncertainty scale factor for correlated noise from Fruchter & Hook (2002).

        Parameters
        ----------
        natural_pixsize : float
            Natural pixel size of the image in arcsec.

        Returns
        -------
        float
            Uncertainty scale factor.
        """

        r = natural_pixsize / self.pfov

        if r >= 1.0:
            return r / (1.0 - 1.0 / (3.0 * r))
        else:
            return 1.0 / (1.0 - r / 3.0)


def fit_one_psf(obs, psf, i, j):
    """Used by _point_source_uncertainty."""
    x0 = i - obs.image.shape[0]/2
    y0 = j - obs.image.shape[1]/2
    result = minimize(lambda p: np.sum(((obs.image - psf.shifted([x0,y0,p[0]])) / obs.rms) ** 2),
                          [obs.image[i,j]], method='Nelder-Mead')
    return np.sum(psf.shifted([x0,y0,result['x'][0]]))


### Functions used for model fitting ###

def chi2(params, psf, alpha, include_unres, stellarflux, obs, param_limits, model_type, npart):
    """Subtract model from observations and calculate the chi-squared goodness of fit value."""

    model = Model(params, obs.image.shape, obs.pfov, obs.aupp, psf.hires_scale,
                  alpha, include_unres, stellarflux, obs.flux_factor, model_type, npart)

    #impose uniform priors within some ranges;
    #note that the fluxes don't have an upper limit here, to allow for extremely bright cases
    if (model.funres < 0 or model.fres < 0
        or model.r1 <= 0 or model.r2 <= 0
        or model.r1 >= model.r2
        or model.r1 > param_limits.rmax or model.r2 > param_limits.rmax
        or model.cosinc < 0 or model.cosinc > 1
        or model.alpha < param_limits.alphamin
        or model.alpha > param_limits.alphamax
        or abs(model.x0) > param_limits.shiftmax * obs.aupp
        or abs(model.y0) > param_limits.shiftmax * obs.aupp
        or abs(model.theta) > 90):
        return np.inf

    if param_limits.drmax:
        if model.r2 - model.r1 > param_limits.drmax:
            return np.inf

    #force the disc to be at least a model pixel wide if using the geometric model, otherwise
    #unphysical models with just a few pixels scattered around the image can result
    if model_type == ModelType.Geometric:
        dr_pix = (model.r2 - model.r1) * model.cosinc * (psf.hires_scale / obs.aupp)
        if (dr_pix <= 1):
            return np.inf

    model.make_images(psf)

    return np.sum(((obs.image - model.image) / obs.uncert) ** 2)


def log_probability(params, *args):
    """Log-probability to be maximized by MCMC."""

    return -0.5 * chi2(params, *args)


### Main functions ###

def save_params_dict(savepath, dict):
    with open(savepath + '/params.pickle', 'wb') as file:
        pickle.dump(dict, file, protocol = pickle.HIGHEST_PROTOCOL)


def save_params(savepath, resolved, include_unres = None, include_alpha = None, alpha = None,
                param_names = None, max_likelihood = None, median = None,
                lower_uncertainty = None, upper_uncertainty = None, model_consistent = None,
                in_au = None, stellarflux = None, psf_obsid = None,
                psffit_flux = None, psffit_rms = None, pixel_rms = None):
    """Save the main results of the fit in a pickle file."""

    dict = {
        'resolved': resolved,
        'include_unres': include_unres,
        'include_alpha': include_alpha,
        'alpha': alpha,
        'param_names': param_names,
        'max_likelihood': max_likelihood,
        'median': median,
        'lower_uncertainty': lower_uncertainty,
        'upper_uncertainty': upper_uncertainty,
        'model_consistent': model_consistent,
        'in_au': in_au,
        'stellarflux': stellarflux,
        'psf_obsid': psf_obsid,
        'psffit_flux': psffit_flux,
        'psffit_rms': psffit_rms,
        'pixel_rms': pixel_rms
    }

    save_params_dict(savepath, dict)


def run(name_image, name_psf = '', savepath = 'pacs_model/output/', name = '', dist = np.nan,
        stellarflux = 0, boxsize = 13, hires_scale = 3, alpha = 1.5, include_unres = False,
        initial_steps = 100, nwalkers = 200, nsteps = 800, burn = 600, ra = np.nan,
        dec = np.nan, test = False, model_type = ModelType.Particle, npart = 100000,
        query_simbad = False, bg_sub=0, drmax_arcsec=None, gauss_fit=True):
    """Fit one image and save the output."""
    
    # dict where we will save output
    save = {}
    save['image_filename'] = os.path.basename(name_image)
    save['distance'] = dist
    save['ra'] = ra
    save['dec'] = dec
    save['n_bg_sub'] = bg_sub
    save['stellarflux'] = stellarflux
    save['drmax_arcsec'] = drmax_arcsec

    #if given no stellar flux, force an unresolved component to be added
    if (stellarflux == 0 or np.isnan(stellarflux)) and not include_unres:
        include_unres = True
        warnings.warn("No stellar flux was supplied. Forcing the model to include an unresolved flux.",
                      stacklevel = 2)

    save['include_unres'] = include_unres

    # get the data, to get the wavelength, level and rotation,
    # we will re-get below when we have the PSF
    obs = Observation(name_image, target_ra = ra, target_dec = dec, dist = dist, boxsize = boxsize)

    #if no PSF is provided, select one based on the processing level and wavelength
    if name_psf == '':
        if obs.wav != 70 and obs.wav != 100:
            raise Exception(f'No {wav} μm PSF is provided by default')
        if obs.level != 20 and obs.level != 25:
            raise Exception(f'No level {level} PSF is provided by default')

        default_psf = {
            70: {20: 'psf/gamma_dra_70/1342217404/level2/HPPPMAPB/'
                        'hpacs1342217404_20hpppmapb_00_1469423089198.fits.gz',
                 25: 'psf/gamma_dra_70/1342217404/level2_5/HPPHPFMAPB/'
                             'hpacs_25HPPHPFMAPB_blue_1757_p5129_00_v1.0_1470980845846.fits.gz'},
            100: {20: 'psf/gamma_dra_100/1342216069/level2/HPPPMAPB/'
                        'hpacs1342216069_20hpppmapb_00_1469417766626.fits.gz',
                  25: 'psf/gamma_dra_100/1342216069/level2_5/HPPHPFMAPB/'
                              'hpacs_25HPPHPFMAPB_green_1757_p5129_00_v1.0_1470967312171.fits.gz'}
        }

        name_psf = default_psf[obs.wav][obs.level]

    #a boxsize of 13 should be large enough to cover the PSF - no need for a larger PSF
    #even if the image is larger (this would slow down the convolution)
    psf = Observation(name_psf, boxsize = 13, hires_scale = hires_scale, rotate_to = obs.angle,
                      normalize = True)

    save['psf_filename'] = os.path.basename(name_psf)
    save['psf_obsid'] = psf.obsid

    #however, we need to store a PSF with the same dimension as the image for the initial subtraction
    psf_imagesize = Observation(name_psf, boxsize = boxsize, rotate_to = obs.angle, normalize = True)

    #abort execution if the PSF pixel scale doesn't match that of the image
    if not np.isclose(psf.pfov, obs.pfov):
        raise Exception(f"PSF and image pixel sizes do not match ({psf.pfov:.2f} / {obs.pfov:.2f})")

    #abort if the image and PSF are at different wavelengths
    if psf.wav != obs.wav:
        raise Exception("The wavelength of the supplied PSF does not match that of the image"
                        f" ({psf.wav} / {obs.wav})")

    #now get the observation again, using the PSF to estimate uncertainty
    obs = Observation(name_image, target_ra = ra, target_dec = dec, dist = dist, boxsize = boxsize,
                      query_simbad = query_simbad, psf=psf)

    save['image_obsid'] = obs.obsid
    save['wavelength'] = obs.wav
    save['image_level'] = obs.level
    save['psffit_rms'] = obs.psffit_rms
    save['pixel_rms'] = obs.rms
    save['in_au'] = obs.in_au
    save['name'] = name if name != '' else obs.name

    #put the star name, distance, obsid/level & wavelength together into an annotation for the image
    annotation = '\n'.join([f'{obs.wav} μm image (level {(obs.level / 10):g})',
                            f'ObsID: {obs.obsid}',
                            name if name != '' else obs.name])

    if not np.isnan(dist):
        annotation += f' (at {dist:.1f} pc)'

    #ensure that the output directory exists
    if not os.path.exists(savepath):
        os.makedirs(savepath)

    #upper limits on the model parameters
    #NOTE: the radii are restricted to the half-diagonal length of the image - this is a quick
    #way of ensuring that we don't end up with arbitrarily large discs that lie completely outside
    #the image cutout
    #NOTE: all limits are used for the initial differential evolution fit (since this method requires
    #parameter ranges to be specified); for the MCMC fit, fmax is NOT imposed, but the other limits are
    param_limits = ParamLimits(
                               fmax = 200000,                                               #mJy
                               shiftmax = 5,                                                #PACS pixels
                               rmax = min(obs.image.shape) * obs.aupp / np.sqrt(2),         #au
                               imax = 90 if model_type == ModelType.Particle else 88,       #deg
                               alphamin = 0, alphamax = 2,
                               drmax = drmax_arcsec*obs.aupp if drmax_arcsec else None
                              )


    #if requested, first check whether the image is consistent with a PSF and skip the fit if possible
    psfsub = obs.best_psf_subtraction(psf_imagesize, param_limits)
    psffit_flux = np.sum( (obs - psfsub).image ) * obs.flux_factor
    save['psffit_flux'] = psffit_flux

    # iteratively subtract brightest source after PSF subtraction
    save['bg_fluxes'] = np.array([])
    if bg_sub:

        # shift allowed is half image diagonal
        pl_sub = ParamLimits(shiftmax=np.sqrt(2)*psfsub.image.shape[0]/2,
                             fmax=None, rmax=None, imax=None,
                             alphamin=None, alphamax=None, drmax=None)

        # create Observation (since psfsub is only a Plottable)
        sub = copy.copy(obs)
        sub.image = psfsub.image
        sub2 = [sub]

        for i in range(bg_sub):
                tmp1 = sub2[-1].best_psf_subtraction(psf_imagesize, pl_sub)
                tmp2 = copy.copy(obs)
                tmp2.image = tmp1.image
                sub2.append(tmp2)

        # now replace observation and psfsub images with bg subtracted one
        psfsub.image = tmp2.image
        bg_mod = sub - sub2[-1]
        save['bg_fluxes'] = np.append(save['bg_fluxes'], np.sum(bg_mod.image))
        obs.image -= bg_mod.image
        
        
    sig, is_noise = psfsub.consistent_gaussian(radius = 15)
    save['resolved'] = not is_noise

    #do the gaussian fit, after bg subtraction in case this helps
    save['gauss_fit'] = gauss_fit
    if gauss_fit:
        res = obs.best_gauss_fit(param_limits)
        save['gauss_fit_parameters'] = res
        save['gauss_fit_flux'] = np.sum(obs._gaussian_image(res))

    if test:
        if is_noise:
            print(f"The PSF subtraction is consistent with Gaussian noise at the {sig:.0f}% level."
                  " There is likely not a resolved disc here. Skipping this system.")

            print("Exporting image of PSF subtraction...")

            #make a two-panel image: [data, psf subtraction]
            fig, ax = plt.subplots(nrows = 1, ncols = 2, figsize = (9, 6), sharey = True)

            #first plot the PACS data
            obs.plot(ax[0], annotation = annotation)

            #then the PSF subtraction
            psfsub.plot(ax[1], ylabel = False, annotation = 'PSF subtraction')
            psfsub.plot_contours(ax[1], obs.rms)

            plt.tight_layout()
            fig.savefig(savepath + '/image_model.png', dpi = 150)
            plt.close(fig)

            #save a pickle simply indicating that no disc was resolved
            save_params_dict(savepath, save)
#            save_params(savepath, False, psf_obsid = psf.obsid, psffit_flux = psffit_flux,
#                        psffit_rms = obs.psffit_rms, pixel_rms = obs.rms)

            return

        else:
            print(f"The PSF subtraction is not consistent with Gaussian noise at the {sig:.0f}% level."
                  " There may be a resolved disc here. Performing disc fit.")


    #find best-fitting parameters using differential evolution, which searches for the
    #global minimum within the parameter ranges specified by the arguments.
    #format is [<funres,> fres, x0, y0, r1, r2, cosinc, theta]

    search_space = [(0, param_limits.fmax), (0, param_limits.fmax),
                    (-param_limits.shiftmax * obs.aupp, param_limits.shiftmax * obs.aupp),
                    (-param_limits.shiftmax * obs.aupp, param_limits.shiftmax * obs.aupp),
                    (0, param_limits.rmax), (0, param_limits.rmax),
                    (np.cos(np.deg2rad(param_limits.imax)), 1), (-90, 90),
                    (param_limits.alphamin, param_limits.alphamax)]

    pnames = [r'$F_\mathrm{unres}\ /\ \mathrm{mJy}$', r'$F_\mathrm{res}\ /\ \mathrm{mJy}$',
              fr'$x_0\ /\ {obs.sep_unit}$', fr'$y_0\ /\ {obs.sep_unit}$',
              fr'$r_1\ /\ {obs.sep_unit}$', fr'$r_2\ /\ {obs.sep_unit}$',
#              r'$i\ /\ \mathrm{deg}$',
              r'$\cos i$',
              r'$\theta\ /\ \mathrm{deg}$', r'$\alpha$'] #parameter names for plot labels

    save['param_names'] = pnames

    #if not including an unresolved flux, remove the first element of the parameter list
    if not include_unres:
        search_space.pop(0)
        pnames.pop(0)
        pnames[0] = r'$F_\mathrm{disc}\ /\ \mathrm{mJy}$'

    # likewise for last element alpha
    include_alpha = True
    if alpha is not None:
        search_space.pop()
        pnames.pop()
        include_alpha = False

    save['include_alpha'] = include_alpha
    save['alpha'] = alpha

    print("Finding a suitable initial model...")

    pbar = tqdm.tqdm(total = initial_steps)

    #set tol = 0 to ensure that DE runs for the prescribed number of steps & the progress bar works
    res = differential_evolution(chi2, search_space,
                                args = (psf, alpha, include_unres, stellarflux,
                                        obs, param_limits, model_type, npart),
                                updating = 'deferred', workers = -1, #use multiprocessing
                                tol = 0, popsize = 20, maxiter = initial_steps, polish = False,
                                callback = (lambda xk, convergence: pbar.update()))

    pbar.close()

    p0 = res['x']
    ndim = p0.size

    print("Running MCMC sampler...")

    with Pool() as pool:
        sampler = emcee.EnsembleSampler(nwalkers, ndim, log_probability,
                                        args = (psf, alpha, include_unres, stellarflux,
                                                obs, param_limits, model_type, npart),
                                        pool = pool)

        #initialize the walkers with an ndim-dimensional Gaussian distribution
        pos = [p0 + p0 * 0.01 * np.random.randn(ndim) for i in range(nwalkers)]

        #run MCMC
        pos, prob, state = sampler.run_mcmc(pos, nsteps, progress = True)


    print("Pickling samples...")

    #extract and save the samples, excluding burn-in, for future use in e.g. corner plots
    samples = sampler.get_chain(discard = burn, flat = True)

    with open(savepath + '/samples.pickle','wb') as file:
        pickle.dump(samples, file, protocol = pickle.HIGHEST_PROTOCOL)


    print("Exporting plot of MCMC chains...")

    #save a plot of the chains
    fig, ax = plt.subplots(ndim + 1, figsize = (12, 16))
    chain = sampler.get_chain()

    #first plot the parameter chains
    for i in range(ndim):
        ax[i].plot(chain[:, :, i], c = 'k', alpha = 0.3)
        ax[i].set_ylabel(pnames[i])
        ax[i].xaxis.set_major_locator(plt.NullLocator())


    #then the log-probability
    ax[-1].plot(sampler.get_log_prob(), c = 'k', alpha = 0.3)
    ax[-1].xaxis.set_major_locator(MaxNLocator(integer = True))
    ax[-1].set_ylabel('log probability')
    ax[-1].set_xlabel('Step number')

    #formatting common to all subplots
    for i in range(ndim + 1):
        ax[i].axvspan(0, burn - 0.5, alpha = 0.1, color = 'k')
        ax[i].set_xlim(0, nsteps - 1)

    plt.tight_layout(h_pad = 0.5)
#    fig.savefig(savepath + '/chains.pdf')
    fig.savefig(savepath + '/chains.png', dpi = 50)
    plt.close(fig)


    print("Exporting corner plot...")

    # add disk width to corner plot
    samp_pl = samples.copy()
    pnames_pl = pnames.copy()
    if include_unres:
        samp_pl = np.insert(samp_pl, 6, samp_pl[:,5]-samp_pl[:,4], axis=1)
        pnames_pl.insert(6, fr'$\Delta r\ /\ {obs.sep_unit}$')
        samp_pl = np.insert(samp_pl, 6, (samp_pl[:,5]+samp_pl[:,4])/2, axis=1)
        pnames_pl.insert(6, fr'$r_{{avg}}\ /\ {obs.sep_unit}$')
    else:
        samp_pl = np.insert(samp_pl, 5, samp_pl[:,4]-samp_pl[:,3], axis=1)
        pnames_pl.insert(5, fr'$\Delta r\ /\ {obs.sep_unit}$')
        samp_pl = np.insert(samp_pl, 5, (samp_pl[:,4]+samp_pl[:,3])/2, axis=1)
        pnames_pl.insert(5, fr'$r_{{avg}}\ /\ {obs.sep_unit}$')

    #make the corner plot
    fig = corner.corner(samp_pl, quantiles = [0.16, 0.50, 0.84],
                        labels = pnames_pl, show_titles = True, title_fmt = '.1f')
#    fig.savefig(savepath + '/corner.pdf')
    fig.savefig(savepath + '/corner.png', dpi = 150)
    plt.close(fig)


    #get max-likelihood model parameters (no need to exclude burn-in for this, just want the best fit)
    max_likelihood = sampler.flatchain[np.argmax(sampler.flatlnprobability), :]

    #get median and 16th/84th percentile parameters
    median = np.median(samples, axis = 0)
    lower_uncertainty = median - np.percentile(samples, 16, axis = 0)
    upper_uncertainty = np.percentile(samples, 84, axis = 0) - median


    #now make a four-panel image: [data, psf subtraction, high-res max-likelihood model, residuals]
    print("Exporting image of best-fit model...")

    model = Model(max_likelihood, obs.image.shape, obs.pfov, obs.aupp, hires_scale, alpha,
                  include_unres, stellarflux, obs.flux_factor, model_type, npart)

    model.make_images(psf)

    fig, ax = plt.subplots(nrows = 1, ncols = 4, figsize = (18, 6), sharey = True)

    #first plot the PACS data
    obs.plot(ax[0], annotation = annotation)

    #then the PSF subtraction
    psfsub.plot(ax[1], ylabel = False, annotation = 'PSF subtraction')
    psfsub.plot_contours(ax[1], obs.rms)

    #now the high-res model
    annotation_model = 'High-resolution model '
    annotation_model += f'({npart} particles)' if model_type == ModelType.Particle else '(geometric)'
    annotation_model += f'\nUnresolved component{" " if include_unres else " not "}included'

    if not obs.in_au:
        annotation_model += f'\nNo distance{" or stellar flux " if stellarflux == 0 else " "}provided'
    elif stellarflux == 0:
        annotation_model += '\nNo stellar flux provided'

    model.plot(ax[2], ylabel = False, plot_hires = True, annotation = annotation_model,
               scalebar = obs.in_au, dist = dist, log = True)


    #finally, the model residuals
    residual = obs - model
    residual.plot(ax[3], ylabel = False, annotation = 'Residuals')
    residual.plot_contours(ax[3], obs.rms)

    plt.tight_layout()
    fig.savefig(savepath + '/image_model.png', dpi = 150)
    plt.close(fig)


    #check whether the model appears to be a good fit
    sig, is_noise = residual.consistent_gaussian(radius = 15)
    save['fit_ok'] = is_noise

    if is_noise:
        print(f"The residuals are consistent with Gaussian noise at the {sig:.0f}% significance level."
              " The disc model appears to explain the data well.")
    else:
        print(f"The residuals are not consistent with Gaussian noise at the {sig:.0f}% significance level."
              " You may wish to check the residuals for issues.")


    #finally, save the important parameters in a pickle for future analysis
    #note that stellarflux is saved so that we can check whether it was zero & hence how to interpret
    #the model fluxes (i.e. disc flux vs total system flux)
    save['max_likelihood'] = max_likelihood
    save['median'] = median
    save['lower_uncertainty'] = lower_uncertainty
    save['upper_uncertainty'] = upper_uncertainty
    save_params_dict(savepath, save)
#    save_params(savepath, True, include_unres, include_alpha, alpha, pnames,
#                max_likelihood, median, lower_uncertainty, upper_uncertainty,
#                is_noise, obs.in_au, stellarflux, psf.obsid, psffit_flux,
#                obs.psffit_rms, obs.rms)


def parse_args():
    """Parse command-line arguments and return the results as a tuple."""

    parser = argparse.ArgumentParser(description = 'Fit a debris disc model to a Herschel PACS image.',
                                     formatter_class = argparse.RawDescriptionHelpFormatter)

    parser.add_argument('-i', dest = 'img', metavar = 'image_file',
                        help = 'path to FITS file containing image to fit', required = True)
    parser.add_argument('-o', dest = 'output', metavar = 'output',
                        help = 'directory to place output (default ./pacs_model/output/)', default = 'pacs_model/output/')
    parser.add_argument('-n', dest = 'name', metavar = 'name',
                        help = 'name of the star, used as a figure annotation if supplied', default = '')
    parser.add_argument('-p', dest = 'psf', metavar = 'psf_file',
                        help = 'optional path to FITS file containing image to use as PSF', default = '')
    parser.add_argument('-bg', dest = 'bg_sub', type = int, metavar = 'bg_sub',
                        help = 'number of background sources to subtract', default = 0)
    parser.add_argument('-d', dest = 'dist', type = float, metavar = 'distance',
                        help = 'distance in pc (if not provided, disc scale will be in \'\')', default = np.nan)
    parser.add_argument('-f', dest = 'fstar', type = float, metavar = 'stellar_flux',
                        help = 'stellar flux from synthetic photometry in mJy (default 0)', default = 0)
    parser.add_argument('-b', dest = 'boxsize', type = int, metavar = 'boxsize',
                        help = 'image cutout has dimension 2 * boxsize + 1 (default 13)', default = 13)
    parser.add_argument('-m', dest = 'model_scale', type = int, metavar = 'model_scale',
                        help = 'PACS pixel / high-res model pixel size ratio (default 5)', default = 5)
    parser.add_argument('-a', dest = 'alpha', type = float, metavar = 'alpha',
                        help = 'surface brightness profile index (d^-alpha; default 1.5)', default = 1.5)
    parser.add_argument('--drmax', dest = 'drmax', type = float, metavar = 'drmax',
                        help = 'restrict disk width to less than drmax in arcseconds', default = None)
    parser.add_argument('-s', dest = 'initial_steps', type = int, metavar = 'init_steps',
                        help = 'number of steps for initial optimization (default 100)', default = 100)
    parser.add_argument('-mw', dest = 'walkers', type = int, metavar = 'mcwalkers',
                        help = 'number of MCMC walkers (default 200)', default = 200)
    parser.add_argument('-ms', dest = 'steps', type = int, metavar = 'mcsteps',
                        help = 'number of MCMC steps (default 800)', default = 800)
    parser.add_argument('-mb', dest = 'burn', type = int, metavar = 'mcburn',
                        help = 'number of MCMC steps to discard as burn-in (default 600)', default = 600)
    parser.add_argument('-ra', dest = 'ra', type = float, metavar = 'ra',
                        help = 'target right ascension in degrees (optional)', default = np.nan)
    parser.add_argument('-de', dest = 'dec', type = float, metavar = 'dec',
                        help = 'target declination in degrees (optional)', default = np.nan)
    parser.add_argument('--type', dest = 'model_type', metavar = 'type',
                        help = 'model type: g for geometric, p for particle (default p)', default = 'p')
    parser.add_argument('--npart', dest = 'npart', metavar = 'npart', type = int,
                        help = 'number of particles if using model p (default 100000)', default = 100000)
    parser.add_argument('--test', dest = 'testres', action = 'store_true',
                        help = 'only fit if disc appears to be resolved')
    parser.add_argument('--gaussfit', dest = 'gauss', action = 'store_true',
                        help = 'fit 2d Gaussian at image center')
    parser.add_argument('--unres', dest = 'unres', action = 'store_true',
                        help = 'include a component of unresolved flux in the model')
    parser.add_argument('--fitalpha', dest = 'include_alpha', action = 'store_true',
                        help = 'fit for alpha (surface brightness) parameter')
    parser.add_argument('--simbad', dest = 'query_simbad', action = 'store_true',
                        help = 'query simbad for sources near target and plot them')

    args = parser.parse_args()

    #MCMC parameters
    nwalkers = args.walkers
    nsteps = args.steps
    burn = args.burn

    #filenames
    name_image = args.img
    name_psf = args.psf

    #stellar parameters
    dist = args.dist
    stellarflux = args.fstar
    name = args.name
    ra = args.ra
    dec = args.dec

    #model parameters
    initial_steps = args.initial_steps
    boxsize = args.boxsize
    include_unres = args.unres
    model_type_str = args.model_type
    bg_sub = args.bg_sub
    alpha = None if args.include_alpha else args.alpha
    drmax_arcsec = args.drmax

    if model_type_str == 'g':
        model_type = ModelType.Geometric
    elif model_type_str == 'p':
        model_type = ModelType.Particle
    else:
        raise Exception(f"Invalid model type: {model_type_str}")

    npart = args.npart

    #make high-resolution model with (hires_scale*hires_scale) sub-pixels per PACS pixel
    hires_scale = args.model_scale

    #where to save the results
    savepath = args.output

    #use PSF subtraction to test for a resolved disc?
    test = args.testres

    #fit a 2d gaussian to the observation
    gauss_fit = args.gauss

    #overplot SIMBAD sources?
    query_simbad = args.query_simbad

    return (name_image, name_psf, savepath, name, dist, stellarflux, boxsize,
            hires_scale, alpha, include_unres, initial_steps, nwalkers,
            nsteps, burn, ra, dec, test, model_type, npart, query_simbad,
            bg_sub, drmax_arcsec, gauss_fit)


#allow command-line execution
if __name__ == "__main__":
    run(*parse_args())
